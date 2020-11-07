package ru.mephi.spark.data;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Calendar;

@Data
@AllArgsConstructor
public class LinuxLog implements Serializable {
    Calendar logTime;
    int priority;
}
