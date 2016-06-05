package alp.scala.dataanalysis.spark

/**
  * Created by ALPinto on 05/06/2016.
  */

case class Employee(id: Int, name: String)

object DataFrameFromCaseClasses extends App {

  import org.apache.spark.sql.SQLContext
  import org.apache.spark.{SparkConf, SparkContext}

  val sparkConfiguration = new SparkConf()
      .setAppName("colRowDataFrame")
        .setMaster("local[2]")

  val sparkContext = new SparkContext(sparkConfiguration)

  val sqlContext = new SQLContext(sparkContext)

  val listOfEmployees = List(Employee(1, "Arun"), Employee(2, "Jason"), Employee(3, "Abhi"))

  val empFrame = sqlContext.createDataFrame(listOfEmployees)
  empFrame.printSchema()

  // Create DataFrame renaming columns
  val empFrameWithRenamedColumns = sqlContext.createDataFrame(listOfEmployees)
      .withColumnRenamed("id", "empId")
  empFrameWithRenamedColumns.printSchema()


  empFrameWithRenamedColumns.registerTempTable("employeeTable")

  val sortByNameEmployees = sqlContext.sql("select * from employeeTable order by name desc")
  sortByNameEmployees.show()




}
