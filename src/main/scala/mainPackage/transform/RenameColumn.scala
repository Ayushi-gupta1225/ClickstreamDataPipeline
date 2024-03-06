package mainPackage.transform

import com.typesafe.config.Config
import mainPackage.constants.ApplicationConstants
import org.apache.spark.sql._
import org.apache.spark.internal.Logging

object RenameColumn extends Logging {
  def renameColumn(clickStreamLowercase:DataFrame,itemSetLowercase:DataFrame,appConstants: ApplicationConstants):(DataFrame,DataFrame)={
    try
    {
      // renaming column names to their meaningful names of clickStream dataset
      val columnId : Array[String]= appConstants.ID.split(" -> ")
      val columnDeviceType : Array[String]= appConstants.DEVICE_TYPE.split(" -> ")
      val columnSessionId : Array[String]= appConstants.SESSION_ID.split(" -> ")
      val columnRedirectionSource: Array[String] = appConstants.REDIRECTION_SOURCE.split(" -> ")

      val clickStreamRename: DataFrame = clickStreamLowercase.withColumnRenamed(columnId(0), columnId(1))
        .withColumnRenamed(columnDeviceType(0), columnDeviceType(1))
        .withColumnRenamed(columnSessionId(0), columnSessionId(1))
        .withColumnRenamed(columnRedirectionSource(0), columnRedirectionSource(1))

      // renaming column names to their meaningful names of item dataset
      val columnItemPrice: Array[String] = appConstants.ITEM_PRICE.split(" -> ")
      val columnProductType : Array[String]= appConstants.PRODUCT_TYPE.split(" -> ")
      val columnDepartmentName: Array[String] = appConstants.DEPARTMENT_NAME.split(" -> ")

      val itemSetRename: DataFrame = itemSetLowercase.withColumnRenamed(columnItemPrice(0), columnItemPrice(1))
        .withColumnRenamed(columnProductType(0), columnProductType(1))
        .withColumnRenamed(columnDepartmentName(0), columnDepartmentName(1))
      (clickStreamRename, itemSetRename)
    }
    catch {
      case e: Exception =>
        logError("An error occurred during renaming the columns.", e)
        // Returning original DataFrames
        (clickStreamLowercase, itemSetLowercase)
    }
  }
}
