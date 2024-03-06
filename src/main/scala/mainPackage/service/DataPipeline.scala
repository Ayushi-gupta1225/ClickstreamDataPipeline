package mainPackage.service

import com.typesafe.config.Config
import mainPackage.constants.ApplicationConstants
import mainPackage.dataQuality.{ClickStreamItemPrice, ClickstreamEventTimestamp}
import mainPackage.database.DatabaseWrite
import mainPackage.transform.{CastDataTypes, ConvertToLowercase, NullCheck, RemoveDuplicates, RenameColumn}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataPipeline{
  // here the main execution of pipeline starts
  def dataPipeline(spark:SparkSession, config:Config, appConstants: ApplicationConstants):Unit={

    // reading the input files from ApplicationConstants class
    val inputPath_clickStream: String = config.getString(appConstants.CLICK_STREAM_INPUT_PATH)
    val inputPath_itemSet: String = config.getString(appConstants.ITEM_SET_INPUT_PATH)

    // Write processed data to output paths
    val outputPath : String= config.getString(appConstants.JOINED_DATASET)
    val nullPathClickStream: String = config.getString(appConstants.CLICK_STREAM_NULLS)
    val nullPathItemSet: String = config.getString(appConstants.ITEM_SET_NULLS)
    val duplicatesPathClickStream: String = config.getString(appConstants.CLICK_STREAM_DUPLICATES)
    val duplicatesPathItemSet: String = config.getString(appConstants.ITEM_SET_DUPLICATES)
    val invalidItemPrice: String = config.getString(appConstants.INVALID_ITEM_PRICE)
    val invalidEventTimestamp: String = config.getString(appConstants.INVALID_EVENT_TIMESTAMP)

    // calling FileReader to read both input files
    val clickStreamDataframe: DataFrame=FileReader.readDataFrame(spark,inputPath_clickStream)
    val itemSetDataframe: DataFrame = FileReader.readDataFrame(spark, inputPath_itemSet)

    // Show the original dataset with schema
    clickStreamDataframe.printSchema()
    clickStreamDataframe.show()

    itemSetDataframe.printSchema()
    itemSetDataframe.show()

    // calling CastDataTypes object to cast the datatype of columns
    val (clickStreamCast,itemSetCast)=CastDataTypes.castDataTypes(clickStreamDataframe,itemSetDataframe,appConstants)

    // calling NullCheck object to remove columns where null values are present
    val (clickStreamRemoveNull,itemSetRemoveNull)=NullCheck.nullCheck(clickStreamCast,itemSetCast,nullPathClickStream,nullPathItemSet,appConstants)

    // calling RemoveDuplicates object to remove the columns where duplicate values are present
    val (clickStreamDuplicates,itemSetDuplicates)=RemoveDuplicates.removeDuplicates(clickStreamRemoveNull,itemSetRemoveNull,duplicatesPathClickStream,duplicatesPathItemSet,appConstants)

    // calling ConvertToLowercase object to convert all records of a particular column to lowercase
    val (clickStreamLowercase,itemSetLowercase)=ConvertToLowercase.convertToLowercase(clickStreamDuplicates,itemSetDuplicates,appConstants)

    // calling RenameColumn object to rename column names to their meaningful names
    val (clickStreamRename,itemSetRename)=RenameColumn.renameColumn(clickStreamLowercase,itemSetLowercase,appConstants)

    // calling FileWriter object to join the two dataframes and write the final output to a csv file
    val joinedDataframe: DataFrame = FileWriter.fileWriter(clickStreamRename,itemSetRename,outputPath,appConstants)

    // data quality check for item_price
    ClickStreamItemPrice.clickStreamItemPrice(joinedDataframe,appConstants,invalidItemPrice)

    // data quality check for event_timestamp
    ClickstreamEventTimestamp.clickstreamEventTimestamp(joinedDataframe,appConstants,invalidEventTimestamp)

    // calling DatabaseWrite object to write the final dataframe to MySql table named "cdp"
      DatabaseWrite.writeToMySQL(joinedDataframe, "cdp", config, appConstants)
  }
}
