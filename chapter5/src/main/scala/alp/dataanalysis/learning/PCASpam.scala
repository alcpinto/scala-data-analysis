package alp.dataanalysis.learning

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.feature.IDFModel
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithm
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import epic.preprocess.MLSentenceSegmenter
import epic.preprocess.TreebankTokenizer
import org.apache.log4j.Logger
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by apinto on 02/07/16.
  */
object PCASpam extends App {

  val logger = Logger.getLogger(PCASpam.getClass)

  val conf = new SparkConf().setAppName("PCASpam").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)


  case class Document(label: String, content: String)

  // Import data
  val spamCollectionDataLocation = getClass.getResource("/SMSSpamCollection.txt").getPath
  val docs = sc.textFile(spamCollectionDataLocation).map(line => {
    val words = line.split("\t")
    Document(words.head.trim(), words.tail.mkString(" "))
  }).cache()


  val labeledPointsWithTf = getLabeledPoints(docs)
  val lpTfIdf = withIdf(labeledPointsWithTf).cache()


  //Split datasets
  val spamPoints = lpTfIdf.filter(point => point.label == 1).randomSplit(Array(0.8, 0.2))
  val hamPoints = lpTfIdf.filter(point => point.label == 0).randomSplit(Array(0.8, 0.2))

  info("Spam count:" + (spamPoints(0).count) + "::" + (spamPoints(1).count))
  info("Ham count:" + (hamPoints(0).count) + "::" + (hamPoints(1).count))

  val trainingSpamSplit = spamPoints(0)
  val testSpamSplit = spamPoints(1)

  val trainingHamSplit = hamPoints(0)
  val testHamSplit = hamPoints(1)

  val trainingData = trainingSpamSplit ++ trainingHamSplit
  val testData = testSpamSplit ++ testHamSplit

  info ("Training split count : "+trainingData.count())
  info ("Test split count : "+testData.count())


  val unlabeledTrainData = trainingData.map(lpoint => Vectors.dense(lpoint.
    features.toArray)).cache()

  //Scale data - Does not support scaling of SparseVector.
  val scaler = new StandardScaler(withMean = true, withStd = false).
    fit(unlabeledTrainData)
  val scaledTrainingData = scaler.transform(unlabeledTrainData).cache()


  // Extracting the principal components and reduce data
  val trainMatrix = new RowMatrix(scaledTrainingData)
  val pcomp: Matrix = trainMatrix.computePrincipalComponents(100)
  val reducedTrainingData = trainMatrix.multiply(pcomp).rows.cache()

  // Prepare reduced training data
  val reducedTrainingSplit = trainingData.zip(reducedTrainingData).map {
    case (labeled, reduced) => new LabeledPoint(labeled.label, reduced)
  }

  /*
   * Prepare test data to same dimension
   * We just need to make sure that we don't compute the principal components
   * using test data
   */
  val unlabeledTestData = testData.map(lpoint=>lpoint.features)
  val testMatrix = new RowMatrix(unlabeledTestData)
  val reducedTestData =   testMatrix.multiply(pcomp).rows.cache()
  val reducedTestSplit = testData.zip(reducedTestData).map{
    case (labeled, reduced) => new LabeledPoint (labeled.label, reduced)
  }


  // Classify and evaluate the metrics
  val logisticWithBFGS = getAlgorithm(10, 1, 0.001)
  val logisticWithBFGSPredictsActuals = runClassification(logisticWithBFGS, reducedTrainingSplit, reducedTestSplit)
  calculateMetrics(logisticWithBFGSPredictsActuals, "Logistic with BFGS")



  /*
   * Utils methods
   */

  private def getLabeledPoints(docs: RDD[Document]): RDD[LabeledPoint] = {

    //Use Scala NLP - Epic
    val labeledPointsUsingEpicRdd: RDD[LabeledPoint] = docs.mapPartitions { docIter =>

      val segmenter = MLSentenceSegmenter.bundled().get
      val tokenizer = new TreebankTokenizer()
      val hashingTf = new HashingTF(5000)

      docIter.map { doc =>
        val sentences = segmenter.apply(doc.content)
        val features = sentences.flatMap(sentence => tokenizer(sentence))

        //consider only features that are letters or digits and cut off all words that are less than 2 characters
        features.toList.filter(token => token.forall(_.isLetterOrDigit)).filter(_.length() > 1)

        new LabeledPoint(if (doc.label.equals("ham")) 0 else 1, hashingTf.transform(features))
      }
    }.cache()

    labeledPointsUsingEpicRdd

  }


  private def withIdf(lPoints: RDD[LabeledPoint]): RDD[LabeledPoint] = {
    val hashedFeatures = lPoints.map(lp => lp.features)
    val idf: IDF = new IDF()
    val idfModel: IDFModel = idf.fit(hashedFeatures)

    val tfIdf: RDD[Vector] = idfModel.transform(hashedFeatures)

    val lpTfIdf = lPoints.zip(tfIdf).map {
      case (originalLPoint, tfIdfVector) => {
        new LabeledPoint(originalLPoint.label, tfIdfVector)
      }
    }

    lpTfIdf
  }


  private def getAlgorithm(iterations: Int, stepSize: Double, regParam: Double) = {
    val algo = new LogisticRegressionWithLBFGS()
    algo.setIntercept(true).optimizer.setNumIterations(iterations).setRegParam(regParam)
    algo
  }


  private def runClassification(algorithm: GeneralizedLinearAlgorithm[_ <: GeneralizedLinearModel], trainingData: RDD[LabeledPoint],
                        testData: RDD[LabeledPoint]): RDD[(Double, Double)] = {
    val model = algorithm.run(trainingData)
    info ("predicting...")
    val predicted = model.predict(testData.map(point => point.features))
    val actuals = testData.map(point => point.label)
    val predictsAndActuals: RDD[(Double, Double)] = predicted.zip(actuals)
    info (predictsAndActuals.collect.toString)
    predictsAndActuals
  }


  private def calculateMetrics(predictsAndActuals: RDD[(Double, Double)], algorithm: String) {

    val accuracy = 1.0 * predictsAndActuals.filter(predActs => predActs._1 == predActs._2).count() / predictsAndActuals.count()
    val binMetrics = new BinaryClassificationMetrics(predictsAndActuals)
    info(s"************** Printing metrics for $algorithm ***************")
    info(s"Area under ROC ${binMetrics.areaUnderROC}")
    info(s"Accuracy $accuracy")

    val metrics = new MulticlassMetrics(predictsAndActuals)
    info(s"Precision : ${metrics.precision}")
    info(s"Confusion Matrix \n${metrics.confusionMatrix}")
    info(s"************** ending metrics for $algorithm *****************")
  }


  protected def info(message: String) = {
    logger.info(message)
  }

}
