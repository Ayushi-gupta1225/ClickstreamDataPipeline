package transform

import mainPackage.transform.CastDataTypes
import org.scalatest.flatspec.AnyFlatSpec
import utils.sparkReadConfig
import utils.sparkReadConfig.applicationConstants
import org.apache.spark.sql.types.{FloatType, StringType, TimestampType}

class CastDataTypesTest extends AnyFlatSpec {

  "CastDataTypes object" should "do the following"

  it should "cast datatype of columns to desired datatype" in{
    // Reading the dataframes
    val (clickStreamDataframe,itemSetDataframe)=sparkReadConfig.readTestDataframe()
    // Calling the CastDataTypes object
    val (clickStreamCast,itemSetCast) = CastDataTypes.castDataTypes(clickStreamDataframe,itemSetDataframe,applicationConstants)

    // Defining the datatype
    val columnEventTimestamp = applicationConstants.EVENT_TIMESTAMP.split(" -> ")
    val columnItemPrice = applicationConstants.ITEM_PRICE.split(" -> ")
    val str=clickStreamCast.schema(columnEventTimestamp(0)).dataType
    val double=itemSetCast.schema(columnItemPrice(0)).dataType

    // Assert datatype for timestamp column using assertResult
    assertResult(str)(TimestampType)
    // Assert datatype for double column using assertResult
    assertResult(double)(FloatType)

    // Printing the schemas
    println("The schemas of Click Stream Dataset:")
    clickStreamCast.printSchema()
    println("The schemas of Item Dataset:")
    itemSetCast.printSchema()

    // Printing the datasets
    println("The Click Stream Dataset:")
    clickStreamCast.show()
    println("The Item Dataset:")
    itemSetCast.show()
  }
}
