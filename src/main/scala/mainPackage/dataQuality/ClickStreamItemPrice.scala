package mainPackage.dataQuality

import mainPackage.constants.ApplicationConstants
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging

object ClickStreamItemPrice extends Logging {

  def clickStreamItemPrice(dfWithPrices: DataFrame, appConstants:ApplicationConstants, invalidPrice:String): Unit = {
    try
    {
      val lowerLimit = appConstants.DQ_ITEM_PRICE_LOWER_THRESHOLD
      val upperLimit = appConstants.DQ_ITEM_PRICE_UPPER_THRESHOLD


      // Filter out values outside the range (invalid data)
      val columnItemPrice: Array[String] = appConstants.ITEM_PRICE.split(" -> ")

      val invalidData:DataFrame = dfWithPrices.
        filter(col(columnItemPrice(1)) < lowerLimit || col(columnItemPrice(1)) > upperLimit)

      // Save invalid data to a separate file (CSV format)
      invalidData.repartition(1).write.mode("overwrite").option("header", "true").csv(invalidPrice)

      // Summary of Price Range for valid data
      val minPrice = dfWithPrices.agg(min(columnItemPrice(1))).collect()(0)(0)
      val maxPrice = dfWithPrices.agg(max(columnItemPrice(1))).collect()(0)(0)
      println(s"Minimum item price: $minPrice")
      println(s"Maximum item price: $maxPrice")

    }
    catch {
      case ex:Exception =>
        logError("Data quality check for Item Price failed.", ex)
    }
  }
}
