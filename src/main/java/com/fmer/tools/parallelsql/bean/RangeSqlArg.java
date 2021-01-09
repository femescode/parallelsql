package com.fmer.tools.parallelsql.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

/**
 * 范围查询的sql参数
 * @author fengmeng
 * @date 2021/1/9 16:17
 */
@Data
@AllArgsConstructor
public class RangeSqlArg extends SqlArg {
    private long start;
    private long end;
    private boolean isDateType;

    @Override
    public Object[] getArgs() {
        if(isDateType){
            return new Object[]{new Date(start), new Date(end)};
        }else{
            return new Object[]{start, end};
        }
    }
}
