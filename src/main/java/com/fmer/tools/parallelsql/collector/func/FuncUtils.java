package com.fmer.tools.parallelsql.collector.func;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * 函数工具类
 * @author fengmeng
 * @date 2021/1/10 15:09
 */
public class FuncUtils {
    private static final Map<String, AggFunc> funcMap = Maps.newHashMap();
    static {
        funcMap.put("max", new MaxFunc());
        funcMap.put("min", new MinFunc());
        funcMap.put("sum", new SumFunc());
        funcMap.put("count", new CountFunc());
        funcMap.put("group_concat", new GroupConcatFunc());
    }

    private FuncUtils(){}

    public static boolean isAggFunc(String funcName){
        if("avg".equalsIgnoreCase(funcName)){
            throw new RuntimeException("不支持的avg函数，可通过count(*)与sum()来算avg");
        }
        return funcMap.containsKey(funcName.toLowerCase());
    }

    public static AggFunc getAggFunc(String funcName){
        if("avg".equalsIgnoreCase(funcName)){
            throw new RuntimeException("不支持的avg函数，可通过count(*)与sum()来算avg");
        }
        if("max".equalsIgnoreCase(funcName)){
            return new MaxFunc();
        }else if("min".equalsIgnoreCase(funcName)){
            return new MinFunc();
        }else if("sum".equalsIgnoreCase(funcName)){
            return new SumFunc();
        }else if("count".equalsIgnoreCase(funcName)){
            return new CountFunc();
        }else if("group_concat".equalsIgnoreCase(funcName)){
            return new GroupConcatFunc();
        }else{
            throw new RuntimeException("不支持的函数: " + funcName);
        }
    }
}
