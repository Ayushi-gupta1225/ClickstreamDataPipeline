package transform

import mainPackage.transform.ConvertToLowercase
import org.scalatest.flatspec.AnyFlatSpec
import utils.sparkReadConfig.applicationConstants
import utils.sparkReadConfig

class ConvertToLowercaseTest extends AnyFlatSpec {

  "ConvertToLowercase object" should "do the following"

  it should "convert the specified column records to lowercase" in{
    // Reading the dataframes
    val (clickStreamDataframe, itemSetDataframe) = sparkReadConfig.readTestDataframe()
    // Calling ConvertToLowercase object
    val (clickStreamLowercase, itemSetLowercase) = ConvertToLowercase.convertToLowercase(clickStreamDataframe, itemSetDataframe,applicationConstants)

    // Assert lowercase records
    val columnRedirectionSource = applicationConstants.REDIRECTION_SOURCE.split(" -> ")
    val columnDepartmentName = applicationConstants.DEPARTMENT_NAME.split(" -> ")
    assertResult(12)(clickStreamLowercase.select(columnRedirectionSource(0)).count())
    assertResult(12)(itemSetLowercase.select(columnDepartmentName(0)).count())

    // show the datasets
    clickStreamLowercase.show()
    itemSetLowercase.show()
  }
}
