package alp.scala.dataanalysis.spark

/**
  * Created by ALPinto on 05/06/2016.
  */
object DataFramePreparation extends App {

  import org.apache.spark.sql.SQLContext
  import org.apache.spark.{SparkConf, SparkContext}

  val conf = new SparkConf().setAppName("DataWith33Attributes").setMaster("local[2]")
  val sparkContext = new SparkContext(conf)
  val sqlContext = new SQLContext(sparkContext)






}
