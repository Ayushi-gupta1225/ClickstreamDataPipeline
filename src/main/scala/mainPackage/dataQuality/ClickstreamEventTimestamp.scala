package mainPackage.dataQuality

import mainPackage.constants.ApplicationConstants
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging

object ClickstreamEventTimestamp extends Logging {
  def clickstreamEventTimestamp(dfWithTimestamp:DataFrame, appConstants:ApplicationConstants, invalidEvents:String): Unit = {
    try
    { // Define lower and upper limits for timestamp range

      val lowerLimit = appConstants.DQ_EVENT_TIMESTAMP_LOWER_THRESHOLD
      val upperLimit = appConstants.DQ_EVENT_TIMESTAMP_UPPER_THRESHOLD

      // Filter out timestamps outside the range (invalid data)
      val columnEventTimestamp: Array[String] = appConstants.EVENT_TIMESTAMP.split(" -> ")


      val invalidTimestampData:DataFrame = dfWithTimestamp.
        filter(col(columnEventTimestamp(1)) < lowerLimit || col(columnEventTimestamp(1)) > upperLimit)

      // Save invalid timestamp data to a separate file (CSV format)
      invalidTimestampData.repartition(1)
        .write.mode("overwrite").option("header", "true").csv(invalidEvents)

      // Summary of Timestamp Range for valid data
      val minTimestamp = dfWithTimestamp.agg(min(columnEventTimestamp(1))).collect()(0)(0)
      val maxTimestamp = dfWithTimestamp.agg(max(columnEventTimestamp(1))).collect()(0)(0)
      println(s"Minimum timestamp: $minTimestamp")
      println(s"Maximum timestamp: $maxTimestamp")
    }
    catch {
      case ex:Exception=>
        logError("Data quality check for Event Timestamp failed.", ex)
    }
  }
}
