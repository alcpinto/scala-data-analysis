package alp.scala.dataanalysis.spark

import java.sql.Timestamp

/**
  * Created by ALPinto on 10/06/2016.
  */

case class JsonDataModel(name: String, dob: Timestamp, tags: String)

object JSONPreprocesor extends App {

  import org.apache.spark.sql.SQLContext
  import org.apache.spark.{SparkConf, SparkContext}
  import org.joda.time.format.DateTimeFormat
  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  implicit val formats = DefaultFormats

  val sparkConf = new SparkConf().setAppName("JSONPreprocesor").setMaster("local[2]")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)


  // Parsing arbitary date/time inputs and convert an array into a comma-separated string
  val strangeDateFileLoc = getClass.getResource("/StrangeDate.json").getPath
  val stringRDD = sparkContext.textFile(strangeDateFileLoc)
  // Datetime formatter
  val formatter = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")


//  val dataModelRDD = for (json <- stringRDD) {
//    val jsonValue = parse(json)
//    val name = compact(render(jsonValue \ "name")).replace("\"", "")
//    val dateAsString = compact(render(jsonValue \ "dob")).replace("\"", "")
//    val date = new Timestamp(formatter.parseDateTime(dateAsString).getMillis())
//    val tags = render(jsonValue \ "tags").extract[List[String]].mkString(",")
//  } yield JsonDataModel(name, date, tags)

  val dataModelRDD = stringRDD.flatMap(json => {
    val jsonValue = parse(json)
    val name = compact(render(jsonValue \ "name")).replace("\"", "")
    val dateAsString = compact(render(jsonValue \ "dob")).replace("\"", "")
    val date = new Timestamp(formatter.parseDateTime(dateAsString).getMillis())
    val tags = render(jsonValue \ "tags").extract[List[String]].mkString(",")
    Some(JsonDataModel(name, date, tags))
  })

  import sqlContext.implicits._

  val df = dataModelRDD.toDF()
  df.printSchema()
  df.show()



}
