import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.spark.{SparkConf, SparkContext}

object DollarVolume {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(Aggregator.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile(args(0), 24)
    val result = textFile.map(line => line.split(","))
      .map {
        case Array(timestamp, price, volume) =>
          (formatTimestamp(timestamp.toLong), (List(price.toDouble), volume.toDouble))
      }
      .reduceByKey {
        case ((prices1, volume1), (prices2, volume2)) =>
          (prices1 ::: prices2, volume1 + volume2)
      }
      .map {
        case (timestamp, (prices, volume)) =>
          val averagePricePerDay = prices.sum / prices.length
          val dollarVolume = volume * averagePricePerDay
          (timestamp, dollarVolume)
      }
      .cache()
      .sortByKey(ascending = true)
    result.saveAsTextFile(args(1))
  }

  private def formatTimestamp(timestamp: Long): String = {
    val instant = Instant.ofEpochSecond(timestamp)
    LocalDateTime.ofInstant(instant, ZoneId.systemDefault)
      .format(DateTimeFormatter.ofPattern("yyyy/MM/dd"))
    // EEE - DAY OF WEEK (Wed)
  }
}
