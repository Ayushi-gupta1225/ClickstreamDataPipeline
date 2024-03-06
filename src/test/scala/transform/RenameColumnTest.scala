package transform

import mainPackage.transform.RenameColumn
import org.scalatest.flatspec.AnyFlatSpec
import utils.sparkReadConfig
import utils.sparkReadConfig.applicationConstants

class RenameColumnTest extends AnyFlatSpec {

  "RenameColumn object" should "do the following"

  it should "rename columns" in{
    // Reading the dataframes
    val (clickStreamDataframe, itemSetDataframe) = sparkReadConfig.readTestDataframe()
    // Calling the RenameColumn object
    val (clickStreamRename, itemSetRename) = RenameColumn.renameColumn(clickStreamDataframe, itemSetDataframe,applicationConstants)

    // Defining the renamed columns as an array
    val clickStreamExpected=Array("Entity_id","event_timestamp","device_type_t","visitor_session_c","visitor_id","item_id","redirection_source_t")
    val itemSetExpected=Array("item_id","item_unit_price_a","product_type_c","department_n")

    // Asserting the renamed columns
    assertResult(clickStreamExpected)(clickStreamRename.columns)
    assertResult(itemSetExpected)(itemSetRename.columns)

    // Show the datasets
    clickStreamRename.show()
    itemSetRename.show()
  }
}
