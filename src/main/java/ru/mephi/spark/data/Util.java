package ru.mephi.spark.data;
import java.util.Calendar;

/**
 * Util class
 * <p>
 * Parses string log into LinuxLog class
 */
public class Util {
    public static LinuxLog parseLog(String log) {
        String[] splitLog = log.split(" ");
        Calendar logTime = parseDate(splitLog);
        int priority = parsePriority(splitLog[5]);
        return new LinuxLog(logTime, priority);

    };

    private static Calendar parseDate(String[] splitLog){
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MONTH, parseMonth(splitLog[0]));
        calendar.set(Calendar.DATE, Integer.parseInt(splitLog[1]));
        calendar.set(Calendar.HOUR, Integer.parseInt(splitLog[2].split(":")[0]));
        //Minute, time and millisecond not used for reporting
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;
    }

    private static int parseMonth(String month){
        switch (month) {
            case "Jan":
                return 0;
            case "Feb":
                return 1;
            case "Mar":
                return 2;
            case "Apr":
                return 3;
            case "May":
                return 4;
            case "Jun":
                return 5;
            case "Jul":
                return 6;
            case "Aug":
                return 7;
            case "Sep":
                return 8;
            case "Oct":
                return 9;
            case "Nov":
                return 10;
            case "Dec":
                return 11;
            default:
                return -1;
        }
    }
    private static int parsePriority(String priority) {
        switch (priority) {
            case "<debug>":
                return 7;
            case "<info>":
                return 6;
            case "<notice>":
                return 5;
            case "<warning>":
            case "<warn>":
                return 4;
            case "<error>":
            case "<err>":
                return 3;
            case "<crit>":
                return 2;
            case "<alert>":
                return 1;
            case "<emerg>":
            case "<panic>":
                return 0;
            default:
                return 7;
        }
    }
}
