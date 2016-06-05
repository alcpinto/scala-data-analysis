package alp.scala.dataanalysis.spark

/**
  * Created by ALPinto on 05/06/2016.
  */
object DataFrameFromProductClass extends App {

  import alp.scala.dataanalysis.spark.model.Student
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.{SparkConf, SparkContext}


  val conf = new SparkConf().setAppName("DataWith33Attributes").setMaster("local[2]")
  val sparkContext = new SparkContext(conf)
  val sqlContext = new SQLContext(sparkContext)

  val fileLocation = getClass.getResource("/student-mat.csv").getPath
  val rddOfStudents = convertCSVToStudents(fileLocation, sparkContext)

  // Convert to DataFrame
  import sqlContext.implicits._

  val studentsDataFrame = rddOfStudents.toDF();
  studentsDataFrame.printSchema()
  studentsDataFrame.show()


  /**
    * Load and fill an RDD from a file
    * @param filePath
    * @param sc
    * @return
    */
  def convertCSVToStudents(filePath: String, sc: SparkContext) : RDD[Student] = {
    val rddOfStudents: RDD[Student] = sc.textFile(filePath).flatMap(eachLine => Student(eachLine))
    rddOfStudents
  }



}
