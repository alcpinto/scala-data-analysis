package alp.scala.dataanalysis.spark.parquet

/**
  * Created by ALPinto on 06/06/2016.
  */
object ParquetCaseClassMain extends App {

  import alp.scala.dataanalysis.spark.parquet.model.Student
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.{SparkConf, SparkContext}

  val sparkConf = new SparkConf().setAppName("CaseClassToParquet").setMaster("local[2]")

  val sparkContext = new SparkContext(sparkConf)

  val sqlContext = new SQLContext(sparkContext)
  sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

  import org.apache.spark.sql.SaveMode
  import sqlContext.implicits._

  // Convert each line into Student
  val studentsFileLocation = getClass.getResource("/StudentData.csv").getPath
  val rddOfStudents = convertCSVToStudents(studentsFileLocation, sparkContext)

  // Convert RDD[Student] to a Dataframe using sqlContext.implicits
  val studentDFrame = rddOfStudents.toDF()

  // Save DataFrame as Parquet using saveAsParquetFile
  val parquetFileLocation = getClass.getResource("/").getPath + "studentPq.parquet"
  studentDFrame.write.mode(SaveMode.Overwrite).parquet(parquetFileLocation)


  // Read parquet file
  val pqDFrame = sqlContext.read.parquet(parquetFileLocation)
  pqDFrame.show()



  def convertCSVToStudents(filePath: String, sc: SparkContext): RDD[Student] = {
    val rddOfStudents: RDD[Student] = sc.textFile(filePath).flatMap(line => {
      import alp.scala.dataanalysis.spark.parquet.model.Student
      val data = line.split("\\|")
      if (data(0) == "id") None else Some(Student(data(0), data(1), data(2), data(3)))
    })
    rddOfStudents
  }

}
