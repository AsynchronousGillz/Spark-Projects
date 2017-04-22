import org.apache.spark.{SparkConf, SparkContext}

object ScalaExample {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(ScalaExample.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile(args(0))
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))
  }
}
