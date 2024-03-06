package service

import mainPackage.service.FileReader
import org.scalatest.flatspec.AnyFlatSpec
import utils.sparkReadConfig
import utils.sparkReadConfig.{config, applicationConstants}

class FileReaderTest extends AnyFlatSpec {

  "FileReader object" should "do the following"

  it should "read the files in the path" in {
    val (clickStreamTestDataframe,itemSetTestDataframe)=sparkReadConfig.readTestDataframe()

    // Asserting the read dataframes
    assertResult(12)(clickStreamTestDataframe.count())
    assertResult(12)(itemSetTestDataframe.count())

    // Show the datasets
    clickStreamTestDataframe.show()
    itemSetTestDataframe.show()
  }
}
