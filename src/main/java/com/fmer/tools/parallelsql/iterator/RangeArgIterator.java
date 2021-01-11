package com.fmer.tools.parallelsql.iterator;

import com.fmer.tools.parallelsql.bean.ArgLocation;
import com.fmer.tools.parallelsql.bean.CliArgs;
import com.fmer.tools.parallelsql.bean.RangeSqlArg;
import com.fmer.tools.parallelsql.utils.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Iterator;

/**
 * 范围型参数迭代器
 * @author fengmeng
 * @date 2021/1/9 15:53
 */
public class RangeArgIterator implements Iterator<RangeSqlArg> {

    private CliArgs cliArgs;
    private long start;
    private long end;
    private long curr;
    private long range;
    private boolean isDateType;

    public RangeArgIterator(CliArgs cliArgs){
        this.cliArgs = cliArgs;
        String rangeStart = cliArgs.getRangeStart();

        if(DateUtils.isDateString(rangeStart)){
            this.start = DateUtils.parseDate(rangeStart).getTime();
            this.end = DateUtils.parseDate(cliArgs.getRangeEnd()).getTime();
            this.curr = this.start;
            this.range = getSpan(cliArgs.getRangeSpan(), rangeStart);
            this.isDateType = true;
        }else if(NumberUtils.isDigits(rangeStart)){
            this.start = Long.parseLong(rangeStart);
            this.end = Long.parseLong(cliArgs.getRangeEnd());
            this.curr = this.start;
            this.range = getSpan(cliArgs.getRangeSpan(), rangeStart);
            this.isDateType = false;
        }else{
            throw new RuntimeException("无法识别的rangeStart: " + rangeStart);
        }
    }

    private long getSpan(String rangeSpan, String rangeStart){
        if(StringUtils.isEmpty(rangeSpan)){
            return 1L;
        }
        if(NumberUtils.isDigits(rangeSpan)){
            return Long.parseLong(rangeSpan);
        }
        long mills = DateUtils.getTimeSpan(rangeSpan);
        if(DateUtils.isUnixTimestamp(rangeStart)){
            return  mills/ DateUtils.ONE_SECOND_TO_MILLS;
        }
        return mills;
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
        rangeSqlArg.setArgLocation(new ArgLocation(curr - start, end - start));
        return rangeSqlArg;
    }
}
