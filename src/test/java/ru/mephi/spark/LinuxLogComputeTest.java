package ru.mephi.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.mephi.spark.data.LinuxLog;
import ru.mephi.spark.data.LogCounts;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.mephi.spark.data.Util.parseLog;


public class LinuxLogComputeTest {

    private JavaSparkContext sparkContext;

    private final List<LinuxLog> input = new ArrayList<>();
    private final List<LogCounts> expectedOutput = new ArrayList<>();

    @BeforeEach
    public void init() throws ParseException, IOException {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("junit");
        sparkContext = new JavaSparkContext(conf);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Files.readAllLines(new File("src/test/resources/logs").toPath(), Charset.defaultCharset()).forEach(s -> input.add(parseLog(s)));

        expectedOutput.add(new LogCounts(dateToCalendar(dateFormat.parse("2020-11-05 00:00:00")), 0, 6));
        expectedOutput.add(new LogCounts(dateToCalendar(dateFormat.parse("2020-11-05 00:00:00")), 4, 4));
        expectedOutput.add(new LogCounts(dateToCalendar(dateFormat.parse("2020-11-05 00:00:00")), 2, 3));
        expectedOutput.add(new LogCounts(dateToCalendar(dateFormat.parse("2020-11-05 15:00:00")), 5, 2));
        expectedOutput.add(new LogCounts(dateToCalendar(dateFormat.parse("2020-11-05 15:00:00")), 3, 2));
        expectedOutput.add(new LogCounts(dateToCalendar(dateFormat.parse("2020-11-05 15:00:00")), 0, 3));
    }

    @Test
    public void testLogAggregation() {
        JavaRDD<LinuxLog> metricRdd = sparkContext.parallelize(input, 2);
        JavaRDD<LogCounts> resultRdd = LinuxLogCompute.countLogs(metricRdd);
        List<LogCounts> resultList = resultRdd.collect();
        assertEquals(expectedOutput.size(), resultList.size());
        assertTrue(resultList.containsAll(expectedOutput));
    }

    private Calendar dateToCalendar(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.setTime(date);
        return calendar;
    }
}