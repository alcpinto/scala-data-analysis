package alp.scala.dataanalysis.spark

import com.databricks.spark.csv._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ALPinto on 04/06/2016.
  */
object DataFrameCSV extends App {

  // Spark configuration. "local" means standalone mode
  val conf = new SparkConf().setAppName("csvDataFrame").setMaster("local[2]")

  // Spark context based on spark configuration
  val sparkContext = new SparkContext(conf)

  // SQL context for spark context defined above
  val sqlContext = new SQLContext(sparkContext)

  // Load DataFrame source
  // Get file location
  val studentsFileLocation = getClass().getResource("/StudentData.csv").getPath
  val students = sqlContext.csvFile(filePath = studentsFileLocation, useHeader = true, delimiter = '|')

  // Alternative way to load data from file
  // val options=Map("header"->"true", "path"->"ModifiedStudent.csv")
  // val newStudents=sqlContext.load("com.databricks.spark.csv",options)

  // print DataFrame schema
  students.printSchema()

  // Sample some rows from DataFrame. Default is 20
  students.show(5)
  /*
   * Alternative way to get some rows
   *  - students.head(5).foreach(println)
   *  or
   *  - students.take(5).foreach(println)
   */

  // Select some columns from original DataFrame
  val emailDataFrame = students.select("email")
  emailDataFrame.show(3)

  val studentEmailDF = students.select("studentName", "email")
  studentEmailDF.show(3)

  // Filter data from DataFrame based on conditions
  students.filter("id > 5").show(3) // Can convert a string to a number and perform a numerical comparasion
  // students.filter("email > 'c'").show(3) // returns emails that start with a character greater than 'c'

  students.filter("studentName = ''").show(3) // Filter students without name
  students.filter("studentName = '' OR studentName = 'NULL'").show(3) // Filter students without name or have NULL in name

  // Filtering based on SQL-Like conditions
  students.filter("SUBSTR(studentName, 0, 1) = 'M'").show(3) // selects students whose names start with a 'M'

  // Sort a DataFrame by a particular column - desc
  students.sort(students("studentName").desc).show(5)
  // Sort by more than one column ascending
  students.sort("studentName", "id").show(7)
  // Sort using multiple orders
  students.sort(students("studentName").asc, students("id").desc).show(10)

  // Renaming columns using 'as' function
  val copyOfStudents = students.select(students("studentName").as("name"), students("email"))
  copyOfStudents.show(7)

  /*
   * Treat DataFrames as relational tables and use SQL to query it
   */
  students.registerTempTable("students")
  val dfFilteredBySQL = sqlContext.sql("select * from students where studentName != '' order by email desc")
  dfFilteredBySQL.show(7)


}
