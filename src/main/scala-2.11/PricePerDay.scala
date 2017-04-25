import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.spark.{SparkConf, SparkContext}

object PricePerDay {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(Aggregator.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile(args(0), 24)
    val result = textFile.map(line => line.split(","))
      .map {
        case Array(timestamp, price, _) =>
          (formatTimestamp(timestamp.toLong), List((timestamp.toLong, price.toDouble)))
      }
      .reduceByKey(_ ::: _)
      .map {
        case (timestamp, prices) =>
          val sortedByTimestamp = prices.sorted
          val rateOfChange = sortedByTimestamp.last._2 + sortedByTimestamp.head._2./(2)
          (timestamp, rateOfChange)
      }
      .cache()
      .sortByKey(true)
    result.saveAsTextFile(args(1))
  }

  private def formatTimestamp(timestamp: Long): String = {
    val instant = Instant.ofEpochSecond(timestamp)
    LocalDateTime.ofInstant(instant, ZoneId.systemDefault)
      .format(DateTimeFormatter.ofPattern("yyyy/MM/dd"))
    // EEE - DAY OF WEEK (Wed)
  }
}
