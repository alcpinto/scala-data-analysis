package alp.scala.dataanalysis

import breeze.linalg._
import java.io.File

/**
  * Created by ALPinto on 04/06/2016.
  */
object CSVBreeze extends  App {

  val BASE_CSV_PACKAGE = "/alp/scala/dataanalysis"

  // Get file location
  val strFileLocation = getClass().getResource(BASE_CSV_PACKAGE + "/WWWusage.csv").getPath
  // Read the commma separated file named WWWusage.csv. Skip the header
  val usageMatrix = csvread(file = new File(strFileLocation), separator = ',', skipLines = 1)
  //print first five rows
  println ("Usage matrix \n" + usageMatrix(0 to 5, ::))


  // Get a submatrix by removing the forst column
  val firstColumnSkipped = usageMatrix(::, 1 to usageMatrix.cols-1)
  //Sample some data so as to ensure we are fine
  println ("First Column skipped \n" + firstColumnSkipped(0 to 5, ::))

  // write modified matrix to a new file
  val strWriteFileLocation = getClass().getResource(BASE_CSV_PACKAGE).getPath + "/firstColumnSkipped.csv"
  csvwrite(file = new File(strWriteFileLocation), mat = firstColumnSkipped, separator = ',')

  // Read a vector from saved file
  val strSavedFileLocation = getClass.getResource(BASE_CSV_PACKAGE + "/firstColumnSkipped.csv").getPath
  val vectorFromMatrix = csvread(file = new File(strSavedFileLocation), separator = ',', skipLines = 0)(::, 1)
  println ("Vector from Matrix \n" + vectorFromMatrix.slice(0, 10, 1))

  // Write vector to a file. First we have to convert ir to a matrix
  val strVectorWriteFileLocation = getClass().getResource(BASE_CSV_PACKAGE).getPath + "/firstVector.csv"
  // Transpose in order to get a vertical vector
  csvwrite(file = new File(strVectorWriteFileLocation), mat = vectorFromMatrix.toDenseMatrix.t, separator = ',')


}
