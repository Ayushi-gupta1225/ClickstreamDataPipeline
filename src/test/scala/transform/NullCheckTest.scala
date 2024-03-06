package transform

import mainPackage.transform.NullCheck
import org.scalatest.flatspec.AnyFlatSpec
import utils.sparkReadConfig
import utils.sparkReadConfig.{applicationConstants,config}

class NullCheckTest extends AnyFlatSpec {

  "NullCheck object" should "do the following"

  it should "remove null values" in{
    // Reading the dataframes
    val (clickStreamDataframe, itemSetDataframe) = sparkReadConfig.readTestDataframe()

    // Loading configuration files
    val nullPathClickStream = config.getString(applicationConstants.SAMPLE_CLICK_STREAM_NULLS)
    val nullPathItemSet = config.getString(applicationConstants.SAMPLE_ITEM_SET_NULLS)

    // Calling the NullCheck object
    val (clickStreamRemoveNull, itemSetRemoveNull) = NullCheck.nullCheck(clickStreamDataframe, itemSetDataframe, nullPathClickStream, nullPathItemSet,applicationConstants)

    // Asserting the null records
    assertResult(11)(clickStreamRemoveNull.select("id").count())
    assertResult(11)(itemSetRemoveNull.select("item_id").count())

    // Show the datasets
    clickStreamRemoveNull.show()
    itemSetRemoveNull.show()
  }
}
