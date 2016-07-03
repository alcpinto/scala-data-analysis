package alp.dataanalysis.learning

import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by apinto on 02/07/16.
  */
object BinaryClassificationSpamPipeline extends App {

  val logger = Logger.getLogger(BinaryClassificationSpamPipeline.getClass)

  val conf = new SparkConf().setAppName("BinaryClassificationSpamPipeline").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)


  case class Document(label: Double, content: String)

  val spamCollectionDataLocation = getClass.getResource("/SMSSpamCollection.txt").getPath
  val docs = sc.textFile(spamCollectionDataLocation).map(line => {
    val words = line.split("\t")
    val label=if (words.head.trim().equals("spam")) 1.0 else 0.0
    Document(label, words.tail.mkString(" "))
  })


  //Split dataset
  val spamPoints = docs.filter(doc => doc.label==1.0).randomSplit(Array(0.8, 0.2))
  val hamPoints = docs.filter(doc => doc.label==0.0).randomSplit(Array(0.8, 0.2))

  info("Spam count:" + (spamPoints(0).count) + "::" + (spamPoints(1).count))
  info("Ham count:" + (hamPoints(0).count) + "::" + (hamPoints(1).count))


  val trainingSpamSplit = spamPoints(0)
  val testSpamSplit = spamPoints(1)

  val trainingHamSplit = hamPoints(0)
  val testHamSplit = hamPoints(1)

  val trainingSplit = trainingSpamSplit ++ trainingHamSplit
  val testSplit = testSpamSplit ++ testHamSplit


  //Convert documents to Dataframe because the cross validator needs a dataframe
  import sqlContext.implicits._
  val trainingDFrame = trainingSplit.toDF()
  val testDFrame = testSplit.toDF()


  val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("tokens")
  val hashingTf = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol("tf")
  val idf = new IDF().setInputCol(hashingTf.getOutputCol).setOutputCol("tfidf")
  val assembler = new VectorAssembler().setInputCols(Array("tfidf",
    "label")).setOutputCol("features")
  val logisticRegression = new LogisticRegression().setFeaturesCol(assembler.getOutputCol).setLabelCol("label").setMaxIter(10)


  val pipeline = new Pipeline()
  pipeline.setStages(Array(tokenizer, hashingTf, idf, assembler, logisticRegression))

  val model = pipeline.fit(trainingDFrame)


  // No cross validation
  val predictsAndActualsNoCV : RDD[(Double, Double)] = model.transform(testDFrame)
    .select("content", "label", "probability", "prediction")
    .map {
      case Row(content: String, label: Double, prob: Vector, prediction: Double) =>  (label, prediction)
    }.cache()

  calculateMetrics(predictsAndActualsNoCV, "Without Cross validation")

  predictsAndActualsNoCV.collect().foreach( x =>
    if (x._1 != x._2) info(s"A: ${x._1}, P: ${x._2}"))

  //Using Cross validator

  //This will provide the cross validator various parameters to choose from
  val paramGrid = new ParamGridBuilder()
    .addGrid(hashingTf.numFeatures, Array(1000, 5000, 10000))
    .addGrid(logisticRegression.regParam, Array(1, 0.1, 0.03, 0.01))
    .build()


  val crossValidator = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(new BinaryClassificationEvaluator())
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(10)

  val bestModel = crossValidator.fit(trainingDFrame)

  val predictsAndActualsWithCV: RDD[(Double,Double)] = bestModel.transform(testDFrame)
    .map(r => (r.getAs[Double]("label"), r.getAs[Double]("prediction"))).cache
//    .select("content", "label", "probability", "prediction")
//    .map {
//      case Row(text: String, label:Double, prob: Vector, prediction: Double) => (label, prediction)
//    }.cache()

  calculateMetrics(predictsAndActualsWithCV, "Cross validation")



  def calculateMetrics(predictsAndActuals: RDD[(Double, Double)], algorithm: String) {

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


  def info(message: String) = {
    logger.info(message)
  }


}
