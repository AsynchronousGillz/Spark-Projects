import org.apache.spark.{SparkConf, SparkContext}

object MLChange {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(MLChange.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile(args(0), 24)
    val result = textFile.map(line => line.split(","))
      .map {
        case Array(timestamp, price, volume) =>
          price.toDouble + " 1:" + timestamp.toLong + " 2:" + volume.toDouble
      }
      .cache()
    result.saveAsTextFile(args(1))
  }
}
