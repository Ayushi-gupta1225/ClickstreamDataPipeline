package service

import mainPackage.service.FileWriter
import org.scalatest.flatspec.AnyFlatSpec
import utils.sparkReadConfig
import utils.sparkReadConfig.{applicationConstants, config}

class FileWriterTest extends AnyFlatSpec {

  "FileWriter object" should "do the following"

  it should "join the two datasets" in{
    // Reading the test Dataframes
    val (clickStreamDataframe, itemSetDataframe) = sparkReadConfig.readTestDataframe()

    // Loading the test configuration file
    val outputPath = config.getString(applicationConstants.SAMPLE_JOINED_DATASET)

    // Write processed data to output path by calling FileWriter object
    val finalTestDataframe = FileWriter.fileWriter(clickStreamDataframe,itemSetDataframe,outputPath,applicationConstants)

    // Defining the columns of final Dataframe as an array
    val finalDataframeExpected=Array("item_id", "id", "event_timestamp", "device_type", "session_id", "visitor_id", "redirection_source", "item_price", "product_type", "department_name")

    // Asserting the final dataframe columns
    assertResult(finalDataframeExpected)(finalTestDataframe.columns)

  }
}
