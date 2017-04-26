import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.spark.{SparkConf, SparkContext}

object AveragePricePerDay {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(AveragePricePerDay.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile(args(0), 24)
    val result = textFile.map(line => line.split(","))
      .map {
        case Array(timestamp, price, _) =>
          (formatTimestamp(timestamp.toLong), List(price.toDouble))
      }
      .reduceByKey(_ ::: _)
      .cache()
      .map {
        case (timestamp, prices) =>
          val averagePricePerDay = prices.sum / prices.length
          (timestamp, averagePricePerDay)
      }
      .sortByKey(ascending = true)
    result.saveAsTextFile(args(1))
  }

  private def formatTimestamp(timestamp: Long): String = {
    val instant = Instant.ofEpochSecond(timestamp)
    LocalDateTime.ofInstant(instant, ZoneId.systemDefault)
      .format(DateTimeFormatter.ofPattern("EEE"))
    // EEE - DAY OF WEEK (Wed)
  }
}
