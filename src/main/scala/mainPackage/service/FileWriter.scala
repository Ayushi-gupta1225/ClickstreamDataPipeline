package mainPackage.service

import mainPackage.constants.ApplicationConstants
import org.apache.spark.sql._
import org.apache.spark.internal.Logging

object FileWriter extends Logging {
  def fileWriter(clickStreamRename:DataFrame,itemSetRename:DataFrame,outputPath:String, appConstants: ApplicationConstants): DataFrame = {
    try {
      // Join both the dataframes
      val columnItemId = appConstants.ITEM_ID.split(" -> ")

      val ClickStreamEventItemDataframe: DataFrame = clickStreamRename.join(itemSetRename, columnItemId(0))

      // Show the joined dataset schema
      ClickStreamEventItemDataframe.printSchema()
      
      // Show the ClickStreamEventItem DataFrame
      ClickStreamEventItemDataframe.show()

      // Write the ClickStreamEventItem dataframe to a orc file
      ClickStreamEventItemDataframe.repartition(1).write.mode("overwrite").option("header", "true").orc(outputPath)
      ClickStreamEventItemDataframe
    }
    catch {
      case e: Exception =>
        logError(s"No data is there in dataframe to load in MySQL table",e)
        // Returning one of the input DataFrames as an example
        null
    }
  }
}
