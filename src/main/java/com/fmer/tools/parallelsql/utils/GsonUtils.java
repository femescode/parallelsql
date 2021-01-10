package com.fmer.tools.parallelsql.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * gson工具类
 * @author fengmeng
 * @date 2021/1/10 11:53
 */
public class GsonUtils {
    private GsonUtils(){}

    public static Gson getGson(){
        GsonBuilder gb = new GsonBuilder();
        gb.setDateFormat("yyyy-MM-dd HH:mm:ss");
        return gb.create();
    }
}
