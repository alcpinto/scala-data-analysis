package alp.scala.dataanalysis.spark

/**
  * Created by ALPinto on 10/06/2016.
  */
object DataFramePreparation extends App {

  import com.databricks.spark.csv.CsvContext
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.{SparkConf, SparkContext}


  val sparkConf = new SparkConf().setAppName("DataFramePreparation").setMaster("local[2]")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  val students1FileLocation = getClass().getResource("/StudentPrep1.csv").getPath
  val students1 = sqlContext.csvFile(filePath = students1FileLocation, useHeader = true, delimiter = '|')
  val students2FileLocation = getClass().getResource("/StudentPrep2.csv").getPath
  val students2 = sqlContext.csvFile(filePath = students2FileLocation, useHeader = true, delimiter = '|')

  // Merge 2 dataframes
  val allStudents = students1.unionAll(students2)
  println("Merge (union) of 2 DataFrames")
  allStudents.show(allStudents.count().toInt)
  // Intersect 2 dataframes
  val intersection = students1.intersect(students2)
  println("Intersection of 2 DataFrames")
  intersection.foreach(println)
  // Difference (subtraction) between 2 dataframes
  val subtraction = students1.except(students2)
  println("Difference (subtraction) of 2 DataFrames")
  subtraction.foreach(println)
  // Distinct (ignore duplicate elements)
  val distinctStudents = allStudents.distinct
  println("Distinct (ignore duplicate elements)")
//  distinctStudents.foreach(println)
  distinctStudents.show()
  println(distinctStudents.count())

  val sortedCols = allStudents.selectExpr("cast(id as int) as id", "studentName", "phone", "email")
    .sort("id")
  println("Sorting")
  sortedCols.show(sortedCols.count.toInt)


  // Choosing a member from one dataset over another based on predicate

  val idStudentPairs = allStudents.rdd.map(each => (each.getString(0), each))
  //Removes duplicates by id and holds on to the row with the longest name
  val longetsNameRdd = idStudentPairs.reduceByKey((row1, row2) =>
    if (row1.getString(1).length > row2.getString(1).length) row1 else row2
  )
  longetsNameRdd.foreach(println)


}
