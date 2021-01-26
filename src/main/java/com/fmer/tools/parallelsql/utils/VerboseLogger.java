package com.fmer.tools.parallelsql.utils;

import lombok.extern.slf4j.Slf4j;

/**
 * verbose日志
 * @author fengmeng
 * @date 2021/1/11 13:33
 */
@Slf4j
public class VerboseLogger {
    public synchronized static void log(String message){
        log.info(message);
    }
}
