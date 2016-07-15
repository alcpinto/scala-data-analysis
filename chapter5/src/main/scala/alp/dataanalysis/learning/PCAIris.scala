package alp.dataanalysis.learning

import org.apache.log4j.Logger
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by apinto on 03/07/16.
  */
object PCAIris extends App {

  val logger = Logger.getLogger(PCAIris.getClass)

  val conf = new SparkConf().setAppName("PCAIris").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // Import data
  val irisDataLocation = getClass.getResource("/iris.data").getPath
  val data = sc.textFile(irisDataLocation).map(line => {
    val dataArray = line.split(",").take(4)
    Vectors.dense(dataArray.map(_.toDouble))
  })

  // Scale data
  val scaler = new StandardScaler(withMean = true, withStd = false).fit(data)
  val scaledData = scaler.transform(data).cache()

  // Get PCA matrix
  val matrix = new RowMatrix(scaledData)

  // Arriving the number of components
  val svd = matrix.computeSVD(3)
  val sum = svd.s.toArray.sum
  svd.s.toArray.zipWithIndex.foldLeft(0.0) {
    case (cum, (curr, component)) =>
      val percent = (cum + curr) / sum
      println(s"Component and percent ${component + 1} :: $percent :::: Singular value is : $curr")
      cum + curr
  }

  val pcomp: Matrix = matrix.computePrincipalComponents(3)
  val reducedData = matrix.multiply(pcomp).rows

  //principalComponents.

  //Decide number of clusters
  val clusterCost = (1 to 7).map { noOfClusters =>
    val kmeans = new KMeans()
      .setK(noOfClusters)
      .setMaxIterations(5)
      .setInitializationMode(KMeans.K_MEANS_PARALLEL) //KMeans||

    val model = kmeans.run(reducedData)
    (noOfClusters, model.computeCost(reducedData))
  }

  info("Cluster cost on sample data ")
  clusterCost.foreach(x => info(x.toString()))




  protected def info(message: String) = {
    logger.info(message)
  }

}
