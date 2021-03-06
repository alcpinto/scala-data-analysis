package alp.dataanalysis.learning

import org.apache.log4j.Logger
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by apinto on 02/07/16.
  */
object KMeansClusteringIris extends App {

  val logger = Logger.getLogger(KMeansClusteringIris.getClass)

  val conf = new SparkConf().setAppName("KMeansClusteringIris").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)


  // Import data
  val irisDataLocation = getClass.getResource("/iris.data").getPath
  val data = sc.textFile(irisDataLocation).map(line => {
    val dataArray = line.split(",").take(4)
    Vectors.dense(dataArray.map(_.toDouble))
  })


  //Summary statistics before scaling
  val statsBeforeScaling = Statistics.colStats(data)
  info("Statistics before scaling")
  info(s"Max : ${statsBeforeScaling.max}")
  info(s"Min : ${statsBeforeScaling.min}")
  info(s"Mean : ${statsBeforeScaling.mean}")
  info(s"Variance : ${statsBeforeScaling.variance}")

  //Scale data
  val scaler = new StandardScaler(withMean = true, withStd = true).fit(data)
  val scaledData = scaler.transform(data).cache()

  //Summary statistics before scaling
  val statsAfterScaling = Statistics.colStats(scaledData)
  info("Statistics after scaling")
  info(s"Max : ${statsAfterScaling.max}")
  info(s"Min : ${statsAfterScaling.min}")
  info(s"Mean : ${statsAfterScaling.mean}")
  info(s"Variance : ${statsAfterScaling.variance}")

  //Take a sample to come up with the number of clusters
  val sampleData = scaledData.randomSplit(Array(0.8, 0.2))(1)

  //Decide number of clusters
  val clusterCost = (1 to 7).map { noOfClusters =>

    val kmeans = new KMeans()
      .setK(noOfClusters)
      .setMaxIterations(5)
      .setInitializationMode(KMeans.K_MEANS_PARALLEL) //KMeans||

    val model = kmeans.run(sampleData)

    (noOfClusters, model.computeCost(sampleData))

  }

  info("Cluster cost on sample data")
  clusterCost.foreach(cost => info(s"${cost._1}, ${cost._2}"))


  //Let's do the real run for 3 clusters
  val kmeans = new KMeans()
    .setK(3)
    .setMaxIterations(5)
    .setInitializationMode(KMeans.K_MEANS_PARALLEL) //KMeans||

  val model = kmeans.run(scaledData)

  //Cost
  info("Total cost " + model.computeCost(scaledData))
  printClusterCenters(model)

  def printClusterCenters(model: KMeansModel) {
    //Cluster centers
    val clusterCenters: Array[Vector] = model.clusterCenters
    info("Cluster centers")
    clusterCenters.foreach(center => info(center.toString))

  }



  def info(message: String) = {
    logger.info(message)
  }

}
