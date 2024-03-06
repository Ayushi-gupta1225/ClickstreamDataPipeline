package mainPackage.constants

class ApplicationConstants extends Serializable {
  // input path constants
  val CLICK_STREAM_INPUT_PATH: String ="app.input.clickStreamPath"
  val ITEM_SET_INPUT_PATH: String ="app.input.itemSetPath"
  val SAMPLE_CLICK_STREAM_INPUT_PATH: String = "app.input.samplePathClickStream"
  val SAMPLE_ITEM_SET_INPUT_PATH: String = "app.input.samplePathItemSet"

  // output path constants
  val JOINED_DATASET: String ="app.output.joinedDataSetPath"
  val CLICK_STREAM_NULLS: String ="app.output.nullClickStream"
  val ITEM_SET_NULLS: String ="app.output.nullItemSet"
  val CLICK_STREAM_DUPLICATES: String ="app.output.duplicateClickStream"
  val ITEM_SET_DUPLICATES: String ="app.output.duplicateItemSet"
  val INVALID_ITEM_PRICE: String = "app.output.invalidItemPrice"
  val INVALID_EVENT_TIMESTAMP: String = "app.output.invalidEventTimestamp"
  val SAMPLE_JOINED_DATASET: String = "app.output.samplePathOutput"
  val SAMPLE_CLICK_STREAM_NULLS: String = "app.output.sampleNullClickStream"
  val SAMPLE_ITEM_SET_NULLS: String = "app.output.sampleNullItemSet"
  val SAMPLE_CLICK_STREAM_DUPLICATES: String = "app.output.sampleDuplicateClickStream"
  val SAMPLE_ITEM_SET_DUPLICATES: String = "app.output.sampleDuplicateItemSet"

  // spark constants
  val SPARK_MASTER: String ="app.spark.master"
  val SPARK_APP_NAME: String ="app.spark.appName"
  val SPARK_LOG_LEVEL: String ="app.spark.logLevel"

  // jdbc constants
  val JDBC_FORMAT = "jdbc"
  val MODE = "overwrite"
  val DRIVER = "com.mysql.cj.jdbc.Driver"
  val JDBC_URL: String ="app.jdbc.jdbcUrl"

  // constant threshold values for data quality check
  val DQ_ITEM_PRICE_LOWER_THRESHOLD=0.0
  val DQ_ITEM_PRICE_UPPER_THRESHOLD=10000
  val DQ_EVENT_TIMESTAMP_LOWER_THRESHOLD= "2020/01/01 00:00"
  val DQ_EVENT_TIMESTAMP_UPPER_THRESHOLD="2023/12/31 23:59"

  //column constants
  val ID = "id -> Entity_id"
  val EVENT_TIMESTAMP = "event_timestamp -> event_timestamp"
  val DEVICE_TYPE = "device_type -> device_type_t"
  val SESSION_ID = "session_id -> visitor_session_c"
  val VISITOR_ID = "visitor_id -> visitor_id"
  val ITEM_ID = "item_id -> item_id"
  val REDIRECTION_SOURCE = "redirection_source -> redirection_source_t"
  val ITEM_PRICE = "item_price -> item_unit_price_a"
  val PRODUCT_TYPE = "product_type -> product_type_c"
  val DEPARTMENT_NAME = "department_name -> department_n"
}






























//spark-submit --class mainPackage.ClickStreamPipelineMain --num-executors 1 --driver-memory 1G --deploy-mode client --master local --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:C:\\ClickstreamProject\\conf\\log4j.properties" --files C:\\ClickstreamProject\\conf\\log4j.properties C:\\ClickstreamProject\\target\\scala-2.11\\ClickstreamProject-assembly-0.1.0-SNAPSHOT.jar C:\\ClickstreamProject\\conf\\clickStreamLocalConfig.conf