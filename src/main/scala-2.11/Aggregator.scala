import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import org.apache.spark.{SparkConf, SparkContext}

object Aggregator {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(Aggregator.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile(args(0), 24)
    val result = textFile.map(line => line.split(","))
      .map {
        case Array(timestamp, price, volume) =>
          (formatTimestamp(timestamp.toLong), ((price.toDouble, price.toDouble), volume.toDouble))
      }
      .cache()
      .reduceByKey {
        case (((minPrice1, maxPrice1), volume1), ((minPrice2, maxPrice2), volume2)) =>
          ((Math.min(minPrice1, minPrice2), Math.max(maxPrice1, maxPrice2)), volume1 + volume2)
      }
      .sortByKey(ascending = true)
    result.saveAsTextFile(args(1))
  }

  private def formatTimestamp(timestamp: Long): String = {
    val instant = Instant.ofEpochSecond(timestamp)
    LocalDateTime.ofInstant(instant, ZoneId.systemDefault)
      .format(DateTimeFormatter.ofPattern("yyyy/MM"))
  }
}
