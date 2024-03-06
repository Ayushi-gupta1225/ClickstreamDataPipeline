package mainPackage.utils

import com.typesafe.config.Config
import mainPackage.constants.ApplicationConstants
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.internal.Logging

object sparkSession extends Logging {
  def sparkSession(sparkConf:SparkConf, conf:Config, appConstants: ApplicationConstants):SparkSession= {
    // creates a SparkSession
    logInfo(s"Spark Session creation begins")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate() //create or build spark session
    val logLevel = conf.getString(appConstants.SPARK_LOG_LEVEL)
    spark.sparkContext.setLogLevel(logLevel)
    logInfo(s"This is spark.app.id")

    spark
  }
}
