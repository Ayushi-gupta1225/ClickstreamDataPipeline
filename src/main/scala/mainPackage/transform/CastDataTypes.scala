package mainPackage.transform

import com.typesafe.config.Config
import mainPackage.constants.ApplicationConstants
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging

object CastDataTypes extends Logging {
  def castDataTypes(clickStreamDataframe:DataFrame, itemSetDataframe:DataFrame, appConstants: ApplicationConstants):(DataFrame,DataFrame)={
    try
    {
      // casting event_timestamp from string to timestamp datatype
      val columnEventTimestamp: Array[String] = appConstants.EVENT_TIMESTAMP.split(" -> ")

      val clickStreamCast:DataFrame = clickStreamDataframe.withColumn(columnEventTimestamp(0),
        to_timestamp(col(columnEventTimestamp(0)), "MM/dd/yyyy HH:mm"))
      //timeString converted to timestamp with given pattern

      // casting item_price from string to double datatype
      val columnItemPrice: Array[String] = appConstants.ITEM_PRICE.split(" -> ")

      val itemSetCast: DataFrame = itemSetDataframe.
        withColumn(columnItemPrice(0), col(columnItemPrice(0)).cast("float"))

      (clickStreamCast, itemSetCast)
    }
    catch {
      case e: Exception =>
        logError("An error occurred during casting of datatype.", e)
        // Returning original DataFrames
        (clickStreamDataframe, itemSetDataframe)
    }
  }
}
