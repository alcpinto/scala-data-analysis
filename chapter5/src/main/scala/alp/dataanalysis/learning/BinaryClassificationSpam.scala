package alp.dataanalysis.learning

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import epic.preprocess.{MLSentenceSegmenter, TreebankTokenizer}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionWithSGD, SVMWithSGD}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.feature.{HashingTF, IDF, IDFModel}
import org.apache.spark.mllib.regression.{GeneralizedLinearAlgorithm, GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

//Frankly, we could make this a tuple but this looks neat
case class Document(label: String, content: String)


/**
  * Created by apinto on 19/06/16.
  */
object BinaryClassificationSpam extends App {

  val conf = new SparkConf().setAppName("BinaryClassificationSpam").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  /*
   * Load data
   */
  val spamCollectionDataLocation = getClass.getResource("/SMSSpamCollection.txt").getPath
  val docs = sc.textFile(spamCollectionDataLocation).map(line => {
    val words = line.split("\t")
    Document(words.head.trim(), words.tail.mkString(" "))
  })

  // Factoring the inverse document frequency
  val labeledPointsWithTf = getLabeledPoints(docs, "STANFORD")
  val lpTfIdf = withIdf(labeledPointsWithTf).cache()

  // Split dataset
  val spamPoints = lpTfIdf.filter(point => point.label == 1).randomSplit(Array(0.8, 0.2))
  val hamPoints = lpTfIdf.filter(point => point.label == 0).randomSplit(Array(0.8, 0.2))

  println("Spam count:" + (spamPoints(0).count) + "::" + (spamPoints(1).count))
  println("Ham count:" + (hamPoints(0).count) + "::" + (hamPoints(1).count))

  val trainingSpamSplit = spamPoints(0)
  val testSpamSplit = spamPoints(1)
  val trainingHamSplit = hamPoints(0)
  val testHamSplit = hamPoints(1)
  val trainingSplit = trainingSpamSplit ++ trainingHamSplit
  val testSplit = testSpamSplit ++ testHamSplit



  val logisticWithSGD = getAlgorithm("LOGSGD", 100, 1, 0.001)
  val logisticWithBfgs = getAlgorithm("LOGBFGS", 100, 1, 0.001)
  val svmWithSGD = getAlgorithm("SVMSGD", 100, 1, 0.001)


  val logisticWithSGDPredictsActuals=runClassification(logisticWithSGD, trainingSplit, testSplit)
//  val logisticWithBfgsPredictsActuals = runClassification(logisticWithBfgs, trainingSplit, testSplit)
  //val svmWithSGDPredictsActuals=runClassification(svmWithSGD, trainingSplit, testSplit)

  //Calculate evaluation metrics
  calculateMetrics(logisticWithSGDPredictsActuals, "Logistic Regression with SGD")
//  calculateMetrics(logisticWithBfgsPredictsActuals, "Logistic Regression with BFGS")
  //calculateMetrics(svmWithSGDPredictsActuals, "SVM with SGD")


  def runClassification(algorithm: GeneralizedLinearAlgorithm[_ <: GeneralizedLinearModel], trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]): RDD[(Double, Double)] = {
    val model = algorithm.run(trainingData)
    val predicted = model.predict(testData.map(point => point.features))
    val actuals = testData.map(point => point.label)
    val predictsAndActuals: RDD[(Double, Double)] = predicted.zip(actuals)
    predictsAndActuals
  }



  def calculateMetrics(predictsAndActuals: RDD[(Double, Double)], algorithm: String) {

    val accuracy = 1.0 * predictsAndActuals.filter(predActs => predActs._1 == predActs._2).count() / predictsAndActuals.count()
    val binMetrics = new BinaryClassificationMetrics(predictsAndActuals)
    println(s"************** Printing metrics for $algorithm ***************")
    println(s"Area under ROC ${binMetrics.areaUnderROC}")
    //println(s"Accuracy $accuracy")

    val metrics = new MulticlassMetrics(predictsAndActuals)
    val f1=metrics.fMeasure
    println(s"F1 $f1")

    println(s"Precision : ${metrics.precision}")
    println(s"Confusion Matrix \n${metrics.confusionMatrix}")
    println(s"************** ending metrics for $algorithm *****************")
  }



  def getAlgorithm(algo: String, iterations: Int, stepSize: Double, regParam: Double) = algo match {
    case "LOGSGD" => {
      val algo = new LogisticRegressionWithSGD()
      algo.setIntercept(true).optimizer.setNumIterations(iterations).setStepSize(stepSize).setRegParam(regParam)
      algo
    }
    case "LOGBFGS" => {
      val algo = new LogisticRegressionWithLBFGS()
      algo.setIntercept(true).optimizer.setNumIterations(iterations).setRegParam(regParam)
      algo
    }
    case "SVMSGD" => {
      val algo = new SVMWithSGD()
      algo.setIntercept(true).optimizer.setNumIterations(iterations).setStepSize(stepSize).setRegParam(regParam)
      algo
    }
  }



  def withIdf(lPoints: RDD[LabeledPoint]): RDD[LabeledPoint] = {
    val hashedFeatures = labeledPointsWithTf.map(lp => lp.features)
    val idf: IDF = new IDF()
    val idfModel: IDFModel = idf.fit(hashedFeatures)

    val tfIdf: RDD[Vector] = idfModel.transform(hashedFeatures)

    val lpTfIdf = labeledPointsWithTf.zip(tfIdf).map {
      case (originalLPoint, tfIdfVector) => {
        new LabeledPoint(originalLPoint.label, tfIdfVector)
      }
    }

    lpTfIdf
  }



  def getLabeledPoints(docs: RDD[Document], library: String): RDD[LabeledPoint] = library match {

    case "EPIC" => {
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

    case "STANFORD" => {
      def corePipeline(): StanfordCoreNLP = {
        val props = new Properties()
        props.put("annotators", "tokenize, ssplit, pos, lemma")
        new StanfordCoreNLP(props)
      }

      def lemmatize(nlp: StanfordCoreNLP, content: String): List[String] = {
        //We are required to prepare the text as 'annotatable' before we annotate :-)
        val document = new Annotation(content)
        //Annotate
        nlp.annotate(document)
        //Extract all sentences
        val sentences = document.get(classOf[SentencesAnnotation])

        //Extract lemmas from sentences
        import scala.collection.JavaConversions._
        val lemmas = new ArrayBuffer[String]()
        for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
            lemmas += token.get(classOf[LemmaAnnotation])
        }
        //Only lemmas with letters or digits will be considered. Also consider only those words which has a length of at least 2
        lemmas.toList.filter(lemma => lemma.forall(_.isLetterOrDigit)).filter(_.length() > 1)
      }

      val labeledPointsUsingStanfordNLPRdd: RDD[LabeledPoint] = docs.mapPartitions { docIter =>
        val corenlp = corePipeline()
        val stopwordsDataLocation = getClass.getResource("/stopwords.txt").getPath
        val stopwords = Source.fromFile(stopwordsDataLocation).getLines()
        val hashingTf = new HashingTF(5000)

        docIter.map { doc =>
          val lemmas = lemmatize(corenlp, doc.content)
          //remove all the stopwords from the lemma list
          lemmas.filterNot(lemma => stopwords.contains(lemma))

          //Generates a term frequency vector from the features
          val features = hashingTf.transform(lemmas)

          //example : List(until, jurong, point, crazy, available, only, in, bugi, great, world, la, buffet, Cine, there, get, amore, wat)
          new LabeledPoint(
            if (doc.label.equals("ham")) 0 else 1,
            features)

        }
      }.cache()

      labeledPointsUsingStanfordNLPRdd
    }

  }



}
