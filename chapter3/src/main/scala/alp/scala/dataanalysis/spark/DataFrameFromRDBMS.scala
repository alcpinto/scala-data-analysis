package alp.scala.dataanalysis.spark

/**
  * Created by ALPinto on 10/06/2016.
  */
object DataFrameFromRDBMS extends App {

  import com.typesafe.config.ConfigFactory
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.{SparkConf, SparkContext}

  val conf = new SparkConf().setAppName("DataFromRDBMS").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)


  val config = ConfigFactory.load()
  val options = Map(
    "driver" -> config.getString("hsqldb.driver"),
    "url" -> config.getString("hsqldb.connection.url"),
    "dbtable" -> "(select * from PUBLIC.STUDENT) as students",
    "partitionColumn" -> "id",
    "lowerBound" -> "1",
    "upperBound" -> "100",
    "numPartitions"-> "2")

  val dFrame = sqlContext.read.options(options).format("jdbc").load
  dFrame.printSchema()
  dFrame.show()


}
