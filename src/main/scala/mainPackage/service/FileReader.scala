package mainPackage.service

import org.apache.spark.sql._
import org.apache.spark.internal.Logging

object FileReader extends Logging {
  //method to read the input csv file
  def readDataFrame(spark:SparkSession, inputPath:String): DataFrame= {
    try {
      // creating a dataframe that is reading the file
      val dataframe: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath)
      dataframe
    } catch {
      case e: Exception =>
        logError("An error occurred during reading the files",e)
        // returning null dataframe if the file is not read as the data is not present or the input file path is incorrect
        null
    }
  }
}
