package alp.scala.dataanalysis.spark.parquet

import org.apache.spark.serializer.KryoRegistrator
import studentavro.avro.StudentAvro
import com.twitter.chill.avro.AvroSerializer

/**
  * Created by ALPinto on 10/06/2016.
  */
object ParquetAvroSchemaMain extends App {

  import org.apache.hadoop.mapreduce.Job
  import org.apache.parquet.avro.{AvroParquetInputFormat, AvroParquetOutputFormat, AvroWriteSupport}
  import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.{SparkConf, SparkContext}

  import scala.Predef.refArrayOps
  import scala.util.Try
  import scalax.file.Path

  val sparkConf = new SparkConf().setAppName("AvroModelToParquet").setMaster("local[2]")
  sparkConf.set("spark.kryo.registrator", classOf[StudentAvroRegistrator].getName)
  sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val job = new Job()


  val sparkContext = new SparkContext(sparkConf)

  val sqlContext = new SQLContext(sparkContext)
  sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")

  val studentsFileLocation = getClass.getResource("/StudentData.csv").getPath
//  printCSVToStudents(studentsFileLocation, sparkContext)
  val rddOfStudents = convertCSVToStudents(studentsFileLocation, sparkContext)

  val pairRddOfStudentsWithNullKey = rddOfStudents.map(each => (null, each))
  ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
  AvroParquetOutputFormat.setSchema(job, StudentAvro.getClassSchema)

  val studentsWriteFileLocation = getClass.getResource("/").getPath + "studentAvroPq"
  // delete directory if exists
  val path =  Path(studentsWriteFileLocation)
  Try (path.deleteRecursively(continueOnFailure = false))


  pairRddOfStudentsWithNullKey.saveAsNewAPIHadoopFile(studentsWriteFileLocation,
    classOf[Void],
    classOf[StudentAvro],
    classOf[AvroParquetOutputFormat],
    job.getConfiguration())


  // Reading the file back for confirmation
  ParquetInputFormat.setReadSupportClass(job, classOf[AvroWriteSupport])
  val readStudentPair = sparkContext.newAPIHadoopFile(studentsWriteFileLocation,
    classOf[AvroParquetInputFormat[StudentAvro]], classOf[Void], classOf[StudentAvro], job.getConfiguration)

  val justStudentRDD = readStudentPair.map(_._2)
  val studentsAsString = justStudentRDD.collect().take(5).mkString("\n")
  println(studentsAsString)

  /*
   * Utils methods
   */
  def convertCSVToStudents(filePath: String, sc: SparkContext): RDD[StudentAvro] = {
    import studentavro.avro.StudentAvro
    val rddOfStudents: RDD[StudentAvro] = sc.textFile(filePath).flatMap(line => {
      val data = line.split("\\|")
      if (data(0) == "id") None
      else Some(StudentAvro.newBuilder()
          .setId(data(0))
          .setName(data(1))
          .setPhone(data(2))
          .setEmail(data(3)).build())
    })
    rddOfStudents
  }


//  def printCSVToStudents(filePath: String, sc: SparkContext): Void = {
//    sc.textFile(filePath).foreach(line => {
//      val data = line.split("\\|")
//      if (data(0) != "id") {
//        val str = "INSERT INTO PUBLIC.STUDENT (ID, NAME, PHONE, EMAIL) VALUES('" + data(0) + "', '" + data(1) + "', '" + data(2) + "', '" + data(3) + "');"
//        println(str)
//      }
//    })
//    null
//  }





}


class StudentAvroRegistrator extends KryoRegistrator {

  import com.esotericsoftware.kryo.Kryo

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[StudentAvro], AvroSerializer.SpecificRecordBinarySerializer[StudentAvro])
  }
}
