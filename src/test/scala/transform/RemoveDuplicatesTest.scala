package transform

import mainPackage.transform.RemoveDuplicates
import org.scalatest.flatspec.AnyFlatSpec
import utils.sparkReadConfig
import utils.sparkReadConfig.{applicationConstants,config}

class RemoveDuplicatesTest extends AnyFlatSpec {

  "RemoveDuplicates object" should "do the following"

  it should "remove duplicate values" in{
    // Read the dataframes
    val (clickStreamDataframe, itemSetDataframe) = sparkReadConfig.readTestDataframe()

    // Load the configuration files
    val duplicatesPathClickStream = config.getString(applicationConstants.SAMPLE_CLICK_STREAM_DUPLICATES)
    val duplicatesPathItemSet = config.getString(applicationConstants.SAMPLE_ITEM_SET_DUPLICATES)

    // Calling the RemoveDuplicates object
    val (clickStreamRemoveDuplicates, itemSetRemoveDuplicates) = RemoveDuplicates.removeDuplicates(clickStreamDataframe, itemSetDataframe, duplicatesPathClickStream, duplicatesPathItemSet,applicationConstants)

    // Asserting the duplicate records
    val columnId = applicationConstants.ID.split(" -> ")
    val columnItemId = applicationConstants.ITEM_ID.split(" -> ")
    assertResult(8)(clickStreamRemoveDuplicates.select(columnId(0)).count())
    assertResult(8)(itemSetRemoveDuplicates.select(columnItemId(0)).count())

    // Show the datasets
    clickStreamRemoveDuplicates.show()
    itemSetRemoveDuplicates.show()
  }
}
