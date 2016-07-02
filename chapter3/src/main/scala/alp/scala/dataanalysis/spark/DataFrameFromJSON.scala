package alp.scala.dataanalysis.spark

/**
  * Created by ALPinto on 05/06/2016.
  */
object DataFrameFromJSON extends App {

  import org.apache.spark.sql.SQLContext
  import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StringType, StructField, StructType, TimestampType}
  import org.apache.spark.{SparkConf, SparkContext}

  import scala.io.Source
//  import scala.reflect.io.File

  // Define contexts
  val sparkConf = new SparkConf().setAppName("DataFromJSON").setMaster("local[2]")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)


  // Create DataFrame from JSON
//  val jsonLocation = getClass.getResource("/profiles.json").getPath
  val jsonLocation = "hdfs://localhost/data/scalada/profiles.json"
  val dFrame = sqlContext.read.json(jsonLocation)

  dFrame.printSchema()
  dFrame.show()


  // Create DataFrame from JSON using an RDD
  val strRDD = sparkContext.textFile(jsonLocation)
  val jsonDF = sqlContext.read.json(strRDD)
  jsonDF.printSchema()

  // Define our own schema
  val profilesSchema = StructType(
    Seq(
      StructField("_id", StringType, true),
      StructField("about", StringType, true),
      StructField("address", StringType, true),
      StructField("age", IntegerType, true),
      StructField("company", StringType, true),
      StructField("email", StringType, true),
      StructField("eyeColor", StringType, true),
      StructField("favoriteFruit", StringType, true),
      StructField("gender", StringType, true),
      StructField("name", StringType, true),
      StructField("phone", StringType, true),
      StructField("registered", TimestampType, true),
      StructField("tags", ArrayType(StringType), true)
    )
  )
  val jsonDFWithSchema = sqlContext.read
    .schema(profilesSchema)
    .json(strRDD)

  jsonDFWithSchema.printSchema()
  jsonDFWithSchema.show()

  // Filter by timestamp
  jsonDFWithSchema.registerTempTable("profilesTable")
  val filterCount = sqlContext
    .sql("select * from profilesTable where registered > CAST('2014-08-26 00:00:00' AS TIMESTAMP)")
    .count

  val fullCount = sqlContext
    .sql("select * from profilesTable").count

  println("Filtered based on timestamp count: " + filterCount)
  println("Full count: " + fullCount)



  //Writes schema as JSON to file
//  File("profileSchema.json").writeAll(profilesSchema.json)

  val schemaLocation = getClass.getResource("/profileSchema.json").getPath
  val loadedSchema = DataType.fromJson(Source.fromFile(schemaLocation).mkString)
  //Print loaded schema
  println(loadedSchema.prettyJson)


}
