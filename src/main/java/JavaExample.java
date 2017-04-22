import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaExample {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName(JavaExample.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> textFile = sc.textFile(args[0]);
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile(args[1]);
    }
}
