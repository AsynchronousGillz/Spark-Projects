/* BitCoin.java */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.regex.Pattern;

public class BitCoin {

    private static final Pattern COMMA = Pattern.compile(",");

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Usage: BitCoin <file>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(args[0]).cache();

        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaRDD<String[]> words = lines.map(s -> COMMA.split(s));

        JavaPairRDD<String[], String> info = words.mapToPair(s -> new Tuple2<>(s, LocalDateTime.ofEpochSecond(Long.parseLong(s[0]), 0, ZoneOffset.UTC).toString()));

        List<Tuple2<String[], String>> output = info.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._2() + ", " + tuple._1());
        }
        sc.stop();

    }
}
