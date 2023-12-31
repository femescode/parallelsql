package com.fmer.tools.parallelsql.printer;

import com.fmer.tools.parallelsql.bean.ArgLocation;
import com.fmer.tools.parallelsql.utils.VerboseLogger;
import com.google.common.util.concurrent.AtomicDouble;
import lombok.Data;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 用于显示进度
 * @author fengmeng
 * @date 2021/1/10 11:39
 */
@Data
public class Progress {
    private static final long PROGRESS_PRINT_INTERVAL = 1000L;

    private long startTime;
    private AtomicDouble progress;
    private AtomicLong preProgressTime;

    public Progress() {
        this.startTime = System.currentTimeMillis();
        this.progress = new AtomicDouble();
        this.preProgressTime = new AtomicLong();
    }

    public void setProgress(ArgLocation argLocation){
        this.progress.set(((double)argLocation.getOffset() * 100D)/argLocation.getSize());
    }

    public String getProgressToDisplay(){
        preProgressTime.set(System.currentTimeMillis());
        return String.format("[%2.0f%% %s]", progress.get(), getDurationString());
    }

    public void printProgressIfNeed(){
        long currTime = System.currentTimeMillis();
        if(currTime >= preProgressTime.get() + PROGRESS_PRINT_INTERVAL){
            VerboseLogger.log(getProgressToDisplay());
        }
    }

    public void printDoneProgress(){
        progress.set(100D);

        VerboseLogger.log(String.format("[100%% %s]", getDurationString()));
    }

    private String getDurationString(){
        String format = "dd'd'HH'h'mm'm'ss's'";
        long cost = System.currentTimeMillis()-startTime;
        if(cost < 60 * 1000){
            format = "ss's'";
        }else if(cost < 60 * 60 * 1000){
            format = "mm'm'ss's'";
        }else if(cost < 24 * 60 * 60 * 1000){
            format = "HH'h'mm'm'ss's'";
        }
        return DurationFormatUtils.formatDuration(cost, format, false);
    }
}
