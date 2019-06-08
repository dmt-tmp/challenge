package jp.paypay.challenge

import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.expressions.Window.{currentRow, unboundedPreceding}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.concurrent.duration._

object SessionizationJob {

  // Name of fields. We use constants to avoid typos in code that manipulates dataframes
  val timestampField: String = "timestamp"
  val previousTimestampField: String = "previous_timestamp"
  val unixTsField: String = "unix_ts"
  val previousUnixTsField: String = "previous_unix_ts"
  val clientIpField: String = "client_ip"
  val clientIpAndPortField : String = "client_ip_and_port"
  val isNewSessionField: String = "is_new_session"
  val userSessionIdField: String = "user_session_id"
  val sessionTimeField: String = "session_time"
  val requestField: String = "request"
  val urlField: String = "url"
  val nbUrlVisitsField: String = "nb_url_visits"
  val userAgentField: String = "user_agent"

  /* Schema of access log entries, as defined by
     https://docs.aws.amazon.com/elasticloadbalancing/latest/classic/access-log-collection.html#access-log-entry-format
   */
  val accessLogEntriesSchema = new StructType()
    .add(timestampField, TimestampType)
    .add("elb", StringType)
    .add(clientIpAndPortField, StringType)
    .add("backend_ip_and_port", StringType)
    .add("request_processing_time", DoubleType)
    .add("backend_processing_time", DoubleType)
    .add("response_processing_time", DoubleType)
    .add("elb_status_code", ShortType)
    .add("backend_status_code", ShortType)
    .add("received_bytes", IntegerType)
    .add("sent_bytes", IntegerType)
    .add(requestField, StringType)
    .add(userAgentField, StringType)
    .add("ssl_cipher", StringType)
    .add("ssl_protocol", StringType)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()

    val jobArgs: SessionizationJobArgs =
      SessionizationJobArgs.parser.parse(args, SessionizationJobArgs())
       .getOrElse(throw new IllegalArgumentException("Some arguments could not be parsed"))

    val csvOptions: Map[String, String] = Map(
      "delimiter" -> " ",
      "header" -> "false",
      "inferSchema" -> "false"
    )

    val accessLogEntries: DataFrame =
      spark
        .read
        .options(csvOptions)
        .schema(accessLogEntriesSchema)
        .csv(jobArgs.accessLogEntriesPath)

    /* The job will iterate over the following Map:
        - whose key is a folder. Results of SessionizationJob will be saved to s"${jobArgs.baseOutputDirectory}/$folder"
        - whose value is a sequence of fields used to find unique users
     */
    val foldersToSessionizationFieds: Map[String, Seq[String]] =
      Map(
        // Sessionize logs using client IP
        "sessionsByIp" -> Seq(clientIpField),
        // "As a bonus", sessionize logs using client IP and user agent
        "sessionsByIpAndUserAgent" -> Seq(clientIpField, userAgentField)
      )

    foldersToSessionizationFieds.foreach { case (folder, sessionizationFields) =>
      // 1) Sessionize the web logs using "sessionizationFields"
      val accessLogEntriesWithSessions: DataFrame = {
        val df: DataFrame = accessLogEntries.transform(sessionize(sessionizationFields, jobArgs.newSessionThreshold))
        val outputDirectory: String = s"${jobArgs.baseOutputDirectory}/$folder"

        df.coalesce(jobArgs.nbOutputFiles)
          .write
          .mode(SaveMode.Overwrite)
          .parquet(outputDirectory)

        spark.read.parquet(outputDirectory)
      }

      // 2) Determine the average session time
      val usersWithSessionIdsAndTimes: DataFrame =
        accessLogEntriesWithSessions.transform(computeSessionTime(sessionizationFields))

      val averageSessionTimeDS: Dataset[Double] = getAvgSessionTime(usersWithSessionIdsAndTimes)

      averageSessionTimeDS.collect().headOption.foreach { avgSessionTime =>
        println(s"The average session time is $avgSessionTime seconds.")
      }

      // 3) Determine the number of unique URL visits per session
      val usersAndNbVisits: DataFrame = accessLogEntriesWithSessions.transform(computeNbVisits(sessionizationFields))

      usersAndNbVisits.describe(nbUrlVisitsField).show(false)

      // 4) Find the most engaged users, ie the users with the longest session times
      val mostEngagedUsers: DataFrame =
        usersWithSessionIdsAndTimes.transform(getMostEngagedUsers(sessionizationFields, nbUsers = 10))

      mostEngagedUsers.show(false)
    }

  }

  /**
    * @param sessionizationFields fields used to find unique users
    * @param newSessionThreshold cf description of argument "new-session-threshold" in [[SessionizationJobArgs]]
    * @param accessLogEntries access log entries read using [[accessLogEntriesSchema]]
    * @return an access log entries dataframe with new fields:
    *   "client_ip", "previous_timestamp", "unix_ts", "previous_unix_ts", "is_new_session" and "user_session_id"
    */
  def sessionize(sessionizationFields: Seq[String], newSessionThreshold: Duration)(accessLogEntries: DataFrame): DataFrame = {
    // This method is inspired by https://mode.com/blog/finding-user-sessions-sql
    val isNewSession: Column =
      when(col(unixTsField) - col(previousUnixTsField) < lit(newSessionThreshold.toSeconds),
        lit(0)
      ).otherwise(
        lit(1)
      )

    val windowSpec: WindowSpec =
      Window
        .partitionBy(sessionizationFields.map(col): _*)
        .orderBy(timestampField)

    accessLogEntries
      .withColumn(clientIpField,
        split(col(clientIpAndPortField), pattern = ":")(0)
      )
      .withColumn(previousTimestampField,
        lag(timestampField, 1).over(windowSpec)
      )
      .withColumn(unixTsField, unix_timestamp(col(timestampField)))
      .withColumn(previousUnixTsField, unix_timestamp(col(previousTimestampField)))
      .withColumn(isNewSessionField, isNewSession)
      .withColumn(userSessionIdField,
        // Cumulative sum of "is_new_session" creates a unique session id per user
        sum(isNewSession).over(windowSpec.rangeBetween(unboundedPreceding, currentRow))
      )
  }

  /**
    * @param sessionizationFields fields used to find unique users
    * @param dfWithSessionsAndUnixTs a dataframe that contains fields "user_session_id" and "unix_ts"
    * @return a dataframe with fields "user_session_id", "session_time" and the sessionizationFields
    */
  def computeSessionTime(sessionizationFields: Seq[String])(dfWithSessionsAndUnixTs: DataFrame): DataFrame =
    dfWithSessionsAndUnixTs
      .groupBy(userSessionIdField, sessionizationFields: _*)
      .agg(
        (max(unixTsField) - min(unixTsField)).as(sessionTimeField)
      )

  /**
    * @param dfWithSessionTimes a dataframe that contains field "session_time"
    * @return a one-line dataset, containing the average session time
    */
  def getAvgSessionTime(dfWithSessionTimes: DataFrame): Dataset[Double] = {
    import dfWithSessionTimes.sparkSession.implicits._ // needed to call ".as[Double]"

    dfWithSessionTimes
      .select(round(avg(sessionTimeField), scale = 3))
      .as[Double]
  }

  /**
    * @param sessionizationFields fields used to find unique users
    * @param accessLogEntriesWithSessions a dataframe that contains fields "request", "user_session_id" and sessionizationFields
    * @return a dataframe that contains sessionizationFields, fields "user_session_id"
    *   and "nb_url_visits" (the number of distinct URLs that were visited during the session)
    */
  def computeNbVisits(sessionizationFields: Seq[String])(accessLogEntriesWithSessions: DataFrame): DataFrame =
    accessLogEntriesWithSessions
      .withColumn(urlField, split(col(requestField), pattern = " ")(1))
      .groupBy(userSessionIdField, sessionizationFields: _*)
      .agg(countDistinct(urlField).as(nbUrlVisitsField))

  /**
    * @param sessionizationFields fields used to find unique users
    * @param nbUsers the number of users to keep
    * @param usersWithSessionIdsAndTimes
    * @return the top "nbUsers". The returned dataframe contains field "session_time" and sessionizationFields,
    *   and is ordered by decreasing "session_time"
    */
  def getMostEngagedUsers(sessionizationFields: Seq[String], nbUsers: Int)(usersWithSessionIdsAndTimes: DataFrame): DataFrame =
    usersWithSessionIdsAndTimes
      // Only keep the longest session for each user, to avoid returning duplicate users
      .groupBy(sessionizationFields.map(col): _*)
      .agg(max(sessionTimeField).as(sessionTimeField))
      .orderBy(col(sessionTimeField).desc)
      .select(sessionTimeField, sessionizationFields: _*)
      .limit(nbUsers)

}
