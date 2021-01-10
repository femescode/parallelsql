package com.fmer.tools.parallelsql.utils;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.text.ParseException;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * 日期工具类
 * @author fengmeng
 * @date 2021/1/10 11:16
 */
public class DateUtils {
    private static final Pattern DATE_TIME_PATTERN = Pattern.compile("\\d{4}[-/]\\d{2}[-/]\\d{2}(?:[ T]\\d{2}(?::\\d{2}(?::\\d{2})?)?(?:[-+]\\d{2}:?\\d{2})?)?");
    private static final String[] DATE_PATTERNS = new String[]{
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd",
            "yyyy-MM-dd HH:mm:ssZZ",
            "yyyy-MM-dd HH:mm",
            "yyyy-MM-dd HH",
            "yyyy/MM/dd HH:mm:ss",
            "yyyy/MM/dd",
            "yyyy/MM/dd HH:mm:ssZZ",
            "yyyy/MM/dd HH:mm",
            "yyyy/MM/dd HH",
            "yyyy-MM-ddTHH:mm:ss",
            "yyyy-MM-ddTHH:mm:ssZZ",
            "yyyy-MM-ddTHH:mm",
            "yyyy-MM-ddTHH",
    };

    private DateUtils(){}

    public static boolean isDateString(String str){
        return DATE_TIME_PATTERN.matcher(str).matches();
    }

    public static Date parseDate(String str){
        if(str == null){
            return null;
        }
        try {
            return org.apache.commons.lang3.time.DateUtils.parseDate(str, DATE_PATTERNS);
        } catch (ParseException e) {
            return ExceptionUtils.rethrow(e);
        }
    }

    public static long getTimeSpan(String timeSpan){
        if(timeSpan.endsWith("h")){
            return Long.parseLong(timeSpan.substring(0, timeSpan.length() - 1)) * 3600 * 1000;
        }else if(timeSpan.endsWith("m")){
            return Long.parseLong(timeSpan.substring(0, timeSpan.length() - 1)) * 60 * 1000;
        }else if(timeSpan.endsWith("s")){
            return Long.parseLong(timeSpan.substring(0, timeSpan.length() - 1)) * 1000;
        }else{
            throw new RuntimeException("无法识别的timeSpan: " + timeSpan);
        }
    }
}
