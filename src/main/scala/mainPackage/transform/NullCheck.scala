package mainPackage.transform

import mainPackage.constants.ApplicationConstants
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.internal.Logging

object NullCheck extends Logging {
  def nullCheck(clickStreamCast:DataFrame, itemSetCast:DataFrame, nullPathClickStream:String, nullPathItemSet:String, appConstants: ApplicationConstants):(DataFrame,DataFrame)={

    try {
      // drop the records where "id" and "item_id" are null
      val columnId : Array[String]= appConstants.ID.split(" -> ")
      val columnItemId: Array[String] = appConstants.ITEM_ID.split(" -> ")

      val clickStreamNotNull: DataFrame = clickStreamCast.na.drop(Seq(columnId(0)))
      val itemSetNotNull: DataFrame = itemSetCast.na.drop(Seq(columnItemId(0)))

      // defining placeholders for both datasets where null values are present
      val placeholderClickStream = Map(
        "event_timestamp" -> "01-01-1970 00:00",
        "device_type" -> "UNKNOWN",
        "session_id" -> "-1",
        "visitor_id" -> "-1",
        "item_id" -> "-1",
        "redirection_source" -> "N/A"
      )
      val placeholderItemSet = Map(
        "item_price" -> 0.0,
        "product_type" -> "UNKNOWN",
        "department_name" -> "UNKNOWN"
      )

      // filling all null values in other columns with defined placeholders
      val replacedNullClickStream:DataFrame = clickStreamNotNull.na.fill(placeholderClickStream)
      val replacedNullItemSet :DataFrame= itemSetNotNull.na.fill(placeholderItemSet)

      // store the null records in dataframes and into error csv files
      val nullRecordsClickStream:DataFrame = clickStreamCast.filter(col(columnId(0)).isNull)
      val nullRecordsItemSet: DataFrame = itemSetCast.filter(col(columnItemId(0)).isNull)

      // write the filtered records into the error files
      nullRecordsClickStream.repartition(1).write.mode("overwrite").option("header","true").csv(nullPathClickStream)
      nullRecordsItemSet.repartition(1).write.mode("overwrite").option("header","true").csv(nullPathItemSet)

      (replacedNullClickStream,replacedNullItemSet)
    }
    catch {
      case ex: Exception =>
        logError("An error occurred due to null removal.",ex)
        // return the original dataframes
        (clickStreamCast,itemSetCast)
    }
  }
}
