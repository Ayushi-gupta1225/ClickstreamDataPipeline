package mainPackage.transform

import com.typesafe.config.Config
import mainPackage.constants.ApplicationConstants
import org.apache.spark.sql._
import org.apache.spark.internal.Logging

object RemoveDuplicates extends Logging {
  def removeDuplicates(clickStreamRemoveNull:DataFrame, itemSetRemoveNull:DataFrame, duplicatesPathClickStream:String, duplicatesPathItemSet:String, appConstants: ApplicationConstants):(DataFrame,DataFrame)={
    try {
      // remove all duplicate records from primary columns "id" and "item_id"
      val columnId : Array[String]= appConstants.ID.split(" -> ")
      val columnItemId: Array[String] = appConstants.ITEM_ID.split(" -> ")

      val clickStreamDuplicates:DataFrame = clickStreamRemoveNull.dropDuplicates(columnId(0))
      val itemSetDuplicates :DataFrame= itemSetRemoveNull.dropDuplicates(columnItemId(0))

      // storing all duplicate records into another dataframes
      val duplicatesClickStream:DataFrame = clickStreamRemoveNull.except(clickStreamDuplicates)
      val duplicatesItemSet :DataFrame= itemSetRemoveNull.except(itemSetDuplicates)

      // writing the filtered duplicate records to error csv files
      duplicatesClickStream.repartition(1).write.mode("overwrite").option("header", "true").csv(duplicatesPathClickStream)
      duplicatesItemSet.repartition(1).write.mode("overwrite").option("header", "true").csv(duplicatesPathItemSet)

      (clickStreamDuplicates, itemSetDuplicates)
    } catch {
      case e: Exception =>
        logError("An error occurred during duplicate removal.",e)
        // Returning original DataFrames
        (clickStreamRemoveNull, itemSetRemoveNull)
    }
  }
}
