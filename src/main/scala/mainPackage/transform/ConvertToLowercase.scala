package mainPackage.transform

import mainPackage.constants.ApplicationConstants
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging

object ConvertToLowercase extends Logging {
  def convertToLowercase(clickStreamDuplicate:DataFrame, itemSetDuplicate:DataFrame, appConstants: ApplicationConstants):(DataFrame,DataFrame)={
    try
    {
      // converting records of redirection_source to lowercase
      val columnRedirectionSource: Array[String] = appConstants.REDIRECTION_SOURCE.split(" -> ")

      val clickStreamLowercase:DataFrame = clickStreamDuplicate.
        withColumn(columnRedirectionSource(0), lower(col(columnRedirectionSource(0))))

      // converting records of department_name to lowercase
      val columnDepartmentName: Array[String] = appConstants.DEPARTMENT_NAME.split(" -> ")

      val itemSetLowercase:DataFrame = itemSetDuplicate.
        withColumn(columnDepartmentName(0), lower(col(columnDepartmentName(0))))

      (clickStreamLowercase, itemSetLowercase)
    }
    catch {
      case e: Exception =>
        logError("An error occurred during converting to lowercase.", e)
        // Returning original DataFrames as an example
        (clickStreamDuplicate, itemSetDuplicate)
    }
  }
}
