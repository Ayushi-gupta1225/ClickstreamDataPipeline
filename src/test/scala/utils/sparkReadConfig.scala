package utils

import com.typesafe.config.{Config, ConfigFactory}
import mainPackage.constants.ApplicationConstants
import mainPackage.service.FileReader
import org.apache.spark.SparkConf
import org.apache.spark.sql._

import java.io.File

object sparkReadConfig {

  val configPath = "C:\\ClickstreamProject\\conf\\test_application.conf"
  val config: Config = ConfigFactory.parseFile(new File(configPath))
  val applicationConstants = new ApplicationConstants()

  def sparkSession():SparkSession= {
    // Creating a SparkSession
    val sparkConf = readConfig()
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val logLevel = config.getString(applicationConstants.SPARK_LOG_LEVEL)
    spark.sparkContext.setLogLevel(logLevel)
    spark
  }

  def readConfig(): SparkConf = {
    // Reading the configuration file for the test cases
    val sparkAppName = config.getString(applicationConstants.SPARK_APP_NAME)
    val sparkMaster = config.getString(applicationConstants.SPARK_MASTER)
    new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
  }

  def readTestDataframe(): (DataFrame,DataFrame)={
    // Reading the test or sample dataframes
    val spark = sparkSession()

    val inputPathClickStream = config.getString(applicationConstants.SAMPLE_CLICK_STREAM_INPUT_PATH)
    val inputPathItemSet = config.getString(applicationConstants.SAMPLE_ITEM_SET_INPUT_PATH)

    val clickStreamTest = FileReader.readDataFrame(spark, inputPathClickStream)
    val itemSetTest = FileReader.readDataFrame(spark, inputPathItemSet)

    (clickStreamTest,itemSetTest)
  }
}
