package alp.scala.dataanalysis.spark

import com.databricks.spark.csv._
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ALPinto on 04/06/2016.
  */
object DataFrameSqlCSV extends App {

  // Spark configuration. "local" means standalone mode
  val conf = new SparkConf().setAppName("csvDataFrame").setMaster("local[2]")

  // Spark context based on spark configuration
  val sparkContext = new SparkContext(conf)

  // SQL context for spark context defined above
  val sqlContext = new SQLContext(sparkContext)

  /*
   * Treat DataFrames as relational tables and use SQL to query it
   */

  val studentsPrep1FileLocation = getClass().getResource("/StudentPrep1.csv").getPath
  val studentsPrep1 = sqlContext.csvFile(filePath = studentsPrep1FileLocation, useHeader = true, delimiter = '|')

  val studentsPrep2FileLocation = getClass().getResource("/StudentPrep2.csv").getPath
  val studentsPrep2 = sqlContext.csvFile(filePath = studentsPrep2FileLocation, useHeader = true, delimiter = '|')

  // Inner join - default
  val studentsJoin = studentsPrep1.join(studentsPrep2, studentsPrep1("id") === studentsPrep2("id"))
  studentsJoin.show(studentsJoin.count().toInt)

  // Right Outer Join
  val studentsRightOuterJoin = studentsPrep1.join(studentsPrep2, studentsPrep1("id") === studentsPrep2("id"), "right_outer")
  studentsRightOuterJoin.show(studentsRightOuterJoin.count().toInt)

  // Right Outer Join
  val studentsLeftOuterJoin = studentsPrep1.join(studentsPrep2, studentsPrep1("id") === studentsPrep2("id"), "left_outer")
  studentsLeftOuterJoin.show(studentsLeftOuterJoin.count().toInt)


  // Save a DataFrame into a file
  val pathToNewFile = getClass().getResource("/").getPath + "ModifiedStudent.csv"
  println(pathToNewFile)
  val saveOptions = Map("header"->"true", "path"->pathToNewFile)

  val copyOfStudentsPrep2 = studentsPrep2.select(studentsPrep2("studentName").as("name"), studentsPrep2("email"))
  copyOfStudentsPrep2.write
      .format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite).options(saveOptions).save

  // Load saved data
  val newStudents = sqlContext.csvFile(filePath = pathToNewFile, useHeader = true)

  newStudents.printSchema();
  println("New students:")
  newStudents.show()

}
