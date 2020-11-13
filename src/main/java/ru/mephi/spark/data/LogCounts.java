package ru.mephi.spark.data;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Data class for aggregated value
 */
@Data
@AllArgsConstructor
public class LogCounts implements Serializable
{
    Calendar logTime;
    int priority;
    int count;

    @Override
    public String toString() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd HH:mm:ss");
        return dateFormat.format(logTime.getTime()) +  ", " + priority + ", " + count;
    }
}
