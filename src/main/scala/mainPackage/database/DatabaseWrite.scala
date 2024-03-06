package mainPackage.database

import com.typesafe.config.Config
import mainPackage.constants.ApplicationConstants
import org.apache.spark.sql._
import org.apache.spark.internal.Logging
import sys.process._

object DatabaseWrite extends Logging {
  def writeToMySQL(joinedDataFrame: DataFrame, tableName: String, config:Config, appConstants: ApplicationConstants): Unit = {
    try
    {
      val url = config.getString(appConstants.JDBC_URL);
//      val user = config.getString(appConstants.JDBC_USER);
//      val password = config.getString(appConstants.JDBC_PASSWORD);
      val jdbcUser = sys.env.getOrElse("JDBC_USER", "****")
      val jdbcPassword = sys.env.getOrElse("JDBC_PASSWORD", "*********")

      joinedDataFrame.write // initiates the process of writing the DataFrame to MySql
        .format(appConstants.JDBC_FORMAT) // Sets writing format to JDBC
        .mode(appConstants.MODE) // Overwrites existing data in target
        .option("driver", appConstants.DRIVER) // Specifies MySQL JDBC driver
        .option("url", url) // Sets MySQL database URL
        .option("dbtable", tableName) // Specifies target table name
        .option("user", jdbcUser) // Provides MySQL username
        .option("password", jdbcPassword) // Provides MySQL password
        .save() // Executes DataFrame write to MySQL

      }
      catch {
        case e:Exception=>
          logError("An error occurred during loading the data to MySQL table",e)
      }
  }
}
