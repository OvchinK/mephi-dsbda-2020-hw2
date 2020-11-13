package ru.mephi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import ru.mephi.spark.data.LinuxLog;
import ru.mephi.spark.data.LogCounts;
import ru.mephi.spark.data.Util;
import scala.Tuple2;

public class LinuxLogCompute {

    static JavaRDD<LogCounts> countLogs(JavaRDD<LinuxLog> logInfoRDD)
    {
        return logInfoRDD
                //Map log to key-value pairs, using log as key, and integer 1 (count) as value
                .mapToPair((info) -> new Tuple2<>(new LinuxLog(info.getLogTime(), info.getPriority()), 1))
                //Reduce key-value pairs to sum all log counts' per key
                .reduceByKey(Integer::sum)
                //Map key-value pairs to output class objects
                .map((tuple) -> new LogCounts(tuple._1.getLogTime(), tuple._1.getPriority(), tuple._2));
    }

    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf()
                .setAppName("linux-logs");

        //Create a spark context based on created configuration
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> file = sparkContext.textFile("hdfs://localhost:9000/flume/test");

        JavaRDD<LogCounts> logCountsJavaRDD = countLogs(file.map(Util::parseLog));

        logCountsJavaRDD.saveAsTextFile("hdfs://localhost:9000/flume/out");
    }
}
