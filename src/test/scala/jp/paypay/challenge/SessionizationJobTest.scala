package jp.paypay.challenge

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType

import com.holdenkarau.spark.testing.DatasetSuiteBase
import jp.paypay.challenge.SessionizationJob._
import org.scalatest.{DiagrammedAssertions, FunSuite}
// The following imports adds methods to tuples, such as ++ (i.e. concatenation)
// Cf https://github.com/milessabin/shapeless/wiki/Feature-overview:-shapeless-2.0.0#hlist-style-operations-on-standard-scala-tuples
import shapeless.syntax.std.tuple._

class SessionizationJobTest extends FunSuite with DiagrammedAssertions with DatasetSuiteBase {

  import spark.implicits._

  val clientIp1: String = "1.186.179.175"
  val clientIp2: String = "1.186.235.15"
  val clientIp3: String = "203.91.211.44"

  val clientIpAndPort1: String = s"$clientIp1:49345"
  val clientIpAndPort2: String = s"$clientIp2:56962"
  val clientIpAndPort3: String = s"$clientIp3:51402"

  val dummyAccessLogEntries: Seq[(String, String, String)] =
    Seq(
      (clientIpAndPort1, "2015-07-22 17:43:02.643", "GET https://paytm.com:443/papi/nps/merchantrating?merchant_id=32025&channel=web&version=2 HTTP/1.1"),
      (clientIpAndPort1, "2015-07-22 17:43:48.123", "POST https://paytm.com:443/shop/cart HTTP/1.1"),
      (clientIpAndPort1, "2015-07-22 17:44:02.124", "GET https://paytm.com:443/papi/nps/merchantrating?merchant_id=32025&channel=web&version=2 HTTP/1.1"),
      (clientIpAndPort1, "2015-07-22 17:59:58.007", "GET https://paytm.com:443/shop/cart/checkout?channel=web&version=2 HTTP/1.1"),
      (clientIpAndPort1, "2015-07-22 18:00:47.789", "POST https://paytm.com:443/shop/cart HTTP/1.1"),

      (clientIpAndPort2, "2015-07-22 11:05:12.646", "GET https://paytm.com:443/shop/h/electronics?utm_source=Affiliates&utm_medium=VCOMM&utm_campaign=VCOMM-generic&utm_term=24890 HTTP/1.1"),
      (clientIpAndPort2, "2015-07-22 16:17:55.268", "GET https://paytm.com:443/shop?utm_source=Affiliates&utm_medium=VCOMM&utm_campaign=VCOMM&utm_term=24890 HTTP/1.1"),
      (clientIpAndPort2, "2015-07-22 16:24:25.894", "GET https://paytm.com:443/shop/cart?channel=web&version=2 HTTP/1.1"),

      (clientIpAndPort3, "2015-07-22 09:00:27.894", "GET https://paytm.com:443/shop/wallet/txnhistory?page_size=10&page_number=0&channel=web&version=2 HTTP/1.1")
    )

  // "lazy" is needed to avoid a NullPointerException, because "spark" is not yet initialized by spark-testing-base
   lazy val dummyAccessLogEntriesDF: DataFrame =
     dummyAccessLogEntries
      .toDF(clientIpAndPortField, timestampField, requestField)
      .withColumn(timestampField, col(timestampField).cast(TimestampType))

  test("""Method sessionize should return an access log dataframe with new fields:
      |client_ip, previous_timestamp, unix_ts_field, previous_unix_ts_field, is_new_session and user_session_id""".stripMargin) {

    val newFields: Seq[(String, String, Long, Option[Long], Int, Int)] = Seq(
      (clientIp1, null, 1437579782L, None, 1, 1),
      (clientIp1, "2015-07-22 17:43:02.643", 1437579828L, Some(1437579782L), 0, 1),
      (clientIp1, "2015-07-22 17:43:48.123", 1437579842L, Some(1437579828L), 0, 1),
      (clientIp1, "2015-07-22 17:44:02.124", 1437580798L, Some(1437579842L), 1, 2),
      (clientIp1, "2015-07-22 17:59:58.007", 1437580847L, Some(1437580798L), 0, 2),
      (clientIp2, null, 1437555912L, None, 1, 1),
      (clientIp2, "2015-07-22 11:05:12.646", 1437574675L, Some(1437555912L), 1, 2),
      (clientIp2, "2015-07-22 16:17:55.268", 1437575065L, Some(1437574675L), 0, 2),
      (clientIp3, null, 1437548427L, None, 1, 1)
    )

    val expectedDF: DataFrame =
      dummyAccessLogEntries.zip(newFields)
        .map{ case (initialTuple, newTuple) => initialTuple ++ newTuple }
        .toDF(
          clientIpAndPortField,
          timestampField,
          requestField,
          clientIpField,
          previousTimestampField,
          unixTsField,
          previousUnixTsField,
          isNewSessionField,
          userSessionIdField
        )
        .withColumn(timestampField, col(timestampField).cast(TimestampType))
        .withColumn(previousTimestampField, col(previousTimestampField).cast(TimestampType))

    val actualEntriesWithSessions: DataFrame = dummyAccessLogEntriesDF.transform(sessionize(Seq(col(clientIpField))))

    assertDatasetEquals(
      expectedDF.sort(clientIpAndPortField, timestampField),
      actualEntriesWithSessions.sort(clientIpAndPortField, timestampField)
    )
  }
}

