package alp.dataanalysis.learning

import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by apinto on 19/06/16.
  */
object LinearRegressionWine extends App {

  val conf = new SparkConf().setAppName("LinearRegressionWine").setMaster("local[2]")
  val sc = new SparkContext(conf)

  /*
   * Import data
   */
  val wineDataLocation = getClass.getResource("/winequality-red.csv").getPath
  val rdd = sc.textFile(wineDataLocation).map(line => line.split(";"))

  /*
   * Converting each instance into a LabeledPoint
   */
  val dataPoints = rdd.map(row => new LabeledPoint(row.last.toDouble, Vectors.
    dense(row.take(row.length-1).map(str => str.toDouble))))

  /*
   * Prepare training and test data
   */
  val dataSplit = dataPoints.randomSplit(Array(0.8, 0.2))
  val trainingSet = dataSplit(0)
  val testSet = dataSplit(1)

  /*
   * Scaling the features
   */
  val featureVector = rdd.map(row => Vectors.dense(row.take(row.length-1).map(str => str.toDouble)))
  val stats = Statistics.colStats(featureVector)
  println(s"Max : ${stats.max}, Min : ${stats.min}, and Mean : ${stats.mean} and Variance : ${stats.variance}")

  //Same scaler should be apply to training and test data sets
  val scaler = new StandardScaler(withMean = true, withStd = true).fit(trainingSet.map(dp => dp.features))
  val scaledTrainingSet = trainingSet.map(dp => new LabeledPoint(dp.label, scaler.transform(dp.features))).cache()
  val scaledTestSet = testSet.map(dp => new LabeledPoint(dp.label, scaler.transform(dp.features))).cache()

  /*
   * Traning the model
   */
  val regression = algorithm("linear", 1000, 0.5)
  //regression.optimizer.setNumIterations(1000).setStepSize(0.1)

  val model = regression.run(scaledTrainingSet)

  /*
   * Predicting against test data
   */
  val predictions:RDD[Double] = model.predict(scaledTestSet.map(point => point.features))

  /*
   * Evaluating the model.
   * We'll use mean squared error evaluation metrics
   */
  val actuals:RDD[Double] = scaledTestSet.map(_.label)
  val predictsAndActuals: RDD[(Double, Double)] = predictions.zip(actuals)

  val sumSquaredErrors = predictsAndActuals.map{case (pred, act) =>
    println (s"act, pred and difference $act, $pred ${act-pred}")
    math.pow(act-pred, 2)
  }.sum()

  val meanSquaredError = sumSquaredErrors / scaledTestSet.count
  println(s"SSE is $sumSquaredErrors")
  println(s"MSE is $meanSquaredError")

  /*
   * Regularizing the parameters
   */
  def algorithm(algo: String, iterations: Int, stepSize: Double) = algo match {
    case "linear" => {
      val algo = new LinearRegressionWithSGD()
      algo.setIntercept(true).optimizer.setNumIterations(iterations).setStepSize(stepSize)
          .setRegParam(0.001).setMiniBatchFraction(0.05)
      algo
    }
    case "lasso" => {
      val algo = new LassoWithSGD()
      algo.setIntercept(true).optimizer.setNumIterations(iterations).setStepSize(stepSize)
        .setRegParam(0.001).setMiniBatchFraction(0.05)
      algo
    }
    case "ridge" => {
      val algo = new RidgeRegressionWithSGD()
      algo.setIntercept(true).optimizer.setNumIterations(iterations).setStepSize(stepSize)
        .setRegParam(0.001).setMiniBatchFraction(0.05)
      algo
    }
  }

}
