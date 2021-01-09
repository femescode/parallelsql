package com.fmer.tools.parallelsql.iterator;

import com.fmer.tools.parallelsql.bean.CliArgs;
import com.fmer.tools.parallelsql.bean.RangeSqlArg;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * 范围型参数迭代器
 * @author fengmeng
 * @date 2021/1/9 15:53
 */
public class RangeArgIterator implements Iterator<RangeSqlArg> {
    private static final Pattern DATE_TIME_PATTERN = Pattern.compile("\\d{4}[-/]\\d{2}[-/]\\d{2}(?: \\d{2}(?::\\d{2}(?::\\d{2})?)?)?");
    private CliArgs cliArgs;
    private long start;
    private long end;
    private long curr;
    private long range;
    private boolean isDateType;

    public RangeArgIterator(CliArgs cliArgs){
        this.cliArgs = cliArgs;
        String rangeStart = cliArgs.getRangeStart();
        if(DATE_TIME_PATTERN.matcher(rangeStart).matches()){
            try {
                this.start = DateUtils.parseDate(rangeStart, "yyyy-MM-dd HH:mm:ss").getTime();
                this.end = DateUtils.parseDate(cliArgs.getRangeEnd(), "yyyy-MM-dd HH:mm:ss").getTime();
                this.curr = this.start;
                this.range = getSpan(cliArgs.getRangeSpan());
                this.isDateType = true;
            }catch (ParseException e){
                ExceptionUtils.rethrow(e);
            }
        }else if(NumberUtils.isDigits(rangeStart)){
            this.start = Long.parseLong(rangeStart);
            this.end = Long.parseLong(cliArgs.getRangeEnd());
            this.curr = this.start;
            this.range = getSpan(cliArgs.getRangeSpan());
            this.isDateType = false;
        }else{
            throw new RuntimeException("无法识别的rangeStart: " + rangeStart);
        }
    }

    private long getSpan(String rangeSpan){
        if(StringUtils.isEmpty(rangeSpan)){
            return 1L;
        }
        if(NumberUtils.isDigits(rangeSpan)){
            return Long.parseLong(rangeSpan);
        }else if(rangeSpan.endsWith("h")){
            return Long.parseLong(rangeSpan.substring(0, rangeSpan.length() - 1)) * 3600 * 1000;
        }else if(rangeSpan.endsWith("m")){
            return Long.parseLong(rangeSpan.substring(0, rangeSpan.length() - 1)) * 60 * 1000;
        }else if(rangeSpan.endsWith("s")){
            return Long.parseLong(rangeSpan.substring(0, rangeSpan.length() - 1)) * 1000;
        }else{
            throw new RuntimeException("无法识别的rangeSpan: " + rangeSpan);
        }
    }

    @Override
    public boolean hasNext() {
        return curr < end;
    }

    @Override
    public RangeSqlArg next() {
        RangeSqlArg rangeSqlArg;
        if(curr + range >= end){
            rangeSqlArg = new RangeSqlArg(curr, end, isDateType);
            curr = end;
        }else{
            rangeSqlArg = new RangeSqlArg(curr, curr + range, isDateType);
            curr = curr + range;
        }
        return rangeSqlArg;
    }
}
