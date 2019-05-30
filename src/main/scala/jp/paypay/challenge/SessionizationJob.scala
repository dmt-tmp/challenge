package jp.paypay.challenge

import scala.concurrent.duration._

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object SessionizationJob {

  // If a user is inactive for "newSessionThreshold" or more, any future activity is attributed to a new session.
  val newSessionThreshold: Duration = 15.minutes

  // https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html#access-log-entry-format
  val accessLogEntriesSchema = new StructType()
    .add("timestamp", TimestampType)
    .add("elb", StringType)
    .add("client_ip_and_port", StringType)
    .add("backend_ip_and_port", StringType)
    .add("request_processing_time", DoubleType)
    .add("backend_processing_time", DoubleType)
    .add("response_processing_time", DoubleType)
    .add("elb_status_code", ShortType)
    .add("backend_status_code", ShortType)
    .add("received_bytes", IntegerType)
    .add("sent_bytes", IntegerType)
    .add("request", StringType)
    .add("user_agent", StringType)
    .add("ssl_cipher", StringType)
    .add("ssl_protocol", StringType)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // TODO: parse args
    val inputPath: String = args.head

    val csvOptions: Map[String, String] = Map(
      "delimiter" -> " ",
      "header" -> "false",
      "inferSchema" -> "false"
    )

    val accessLogEntries: DataFrame = spark.read.options(csvOptions).csv(inputPath)

    val previousTimestampField: String = "previous_timestamp"
    val timestampField: String = "timestamp"
    val clientIpField: String = "client_ip"

    val isNewSession: Column =
      when(unix_timestamp(col(timestampField)) - unix_timestamp(col(previousTimestampField)) < lit(newSessionThreshold.toSeconds),
        lit(0)
      ).otherwise(
        lit(1)
      )

    accessLogEntries
      .withColumn(clientIpField,
        split($"client_ip_and_port", ":")(0)
      )
      .withColumn(previousTimestampField,
        lag(timestampField, 1).over(Window.partitionBy(clientIpField).orderBy(timestampField))
      )
      .withColumn("is_new_session", isNewSession)
      .withColumn("user_session_id",
        sum(isNewSession).over(Window.partitionBy(clientIpField).orderBy(timestampField))
      )
      .withColumn("global_session_id",
        sum(isNewSession).over(Window.orderBy(clientIpField, timestampField))
      )
  }
}
