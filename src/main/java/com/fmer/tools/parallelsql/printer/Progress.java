package com.fmer.tools.parallelsql.printer;

import com.fmer.tools.parallelsql.bean.ArgLocation;
import com.google.common.util.concurrent.AtomicDouble;
import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 用于显示进度
 * @author fengmeng
 * @date 2021/1/10 11:39
 */
@Data
public class Progress {
    private static final long PROGRESS_PRINT_INTERVAL = 1000L;

    private AtomicDouble progress;
    private AtomicLong preProgressTime;

    public Progress() {
        this.progress = new AtomicDouble();
        this.preProgressTime = new AtomicLong();
    }

    public void setProgress(ArgLocation argLocation){
        this.progress.set(((double)argLocation.getOffset() * 100D)/argLocation.getSize());
    }

    public String getProgressToDisplay(){
        preProgressTime.set(System.currentTimeMillis());
        return String.format("[%2.0f%%]", progress.get());
    }

    public void printProgressIfNeed(){
        long currTime = System.currentTimeMillis();
        if(currTime >= preProgressTime.get() + PROGRESS_PRINT_INTERVAL){
            System.err.println(getProgressToDisplay());
        }
    }

    public void printDoneProgress(){
        progress.set(100D);
        System.err.println("[100%]");
    }
}
