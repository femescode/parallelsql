package com.fmer.tools.parallelsql.utils;

import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 日志ip提供器
 * @author fengmeng
 * @date 2021/1/10 0:47
 */
public class LogIpConvertUtils extends ClassicConverter {
    private final String IP = InetAddress.getLocalHost().getHostAddress();

    public LogIpConvertUtils() throws UnknownHostException {
    }

    @Override
    public String convert(ILoggingEvent iLoggingEvent) {
        return this.IP;
    }
}