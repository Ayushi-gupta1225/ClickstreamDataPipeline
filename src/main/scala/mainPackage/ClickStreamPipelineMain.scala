package mainPackage

import com.typesafe.config.{Config, ConfigFactory}
import mainPackage.constants.ApplicationConstants
import mainPackage.service.DataPipeline
import mainPackage.utils.sparkSession
import org.apache.spark.SparkConf
import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.internal.Logging

object ClickStreamPipelineMain extends Logging {
  def main(args: Array[String]): Unit = {
    try
    {
      // main method where the flow of the code starts
      val JOB_START_TIME: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"))
      logInfo("ClickStream Data Pipeline Process: Beginning  :  " + JOB_START_TIME)

      // read the configuration file from command line arguments
      //val configPath = "C:\\ClickstreamProject\\conf\\clickStreamLocalConfig.conf"
      val configPath: String = args(0)
      val applicationConf: Config = ConfigFactory.parseFile(new File(configPath))
      logInfo(s"$applicationConf")

      // initiating the application constants based on configuration files
      val appConstants : ApplicationConstants = new ApplicationConstants()

      // creating spark session- begin
      val sparkAppName = applicationConf.getString(appConstants.SPARK_APP_NAME)
      val sparkMaster = applicationConf.getString(appConstants.SPARK_MASTER)
      val sparkConf = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
      logInfo(s"This is $sparkConf")

      val spark = sparkSession.sparkSession(sparkConf,applicationConf,appConstants)
      // creating spark session- end

      // DataPipeline execution begin
      DataPipeline.dataPipeline(spark,applicationConf,appConstants)

      val JOB_END_TIME: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"))
      logInfo("ClickStream Data Pipeline Process: Completed  :  " + JOB_END_TIME)
      // Pipeline execution ends
      spark.catalog.clearCache()
      spark.stop()
    }
    catch {
      case e: Exception=>
        logError("An error occurred due to failure of main function ClickStreamPipeline.",e)
    }
  }
}
