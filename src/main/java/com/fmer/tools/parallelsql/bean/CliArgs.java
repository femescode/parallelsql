package com.fmer.tools.parallelsql.bean;

import com.fmer.tools.parallelsql.constants.CollectorEnum;
import com.fmer.tools.parallelsql.constants.DataPrinterEnum;
import com.fmer.tools.parallelsql.constants.StringConstant;
import lombok.Data;

import java.util.Set;

/**
 * 命令行参数对象
 * @author fengmeng
 * @date 2021/1/9 15:07
 */
@Data
public class CliArgs {
    private String host;
    private int port;
    private String database;
    private String username;
    private String password;
    private String sql;
    private int threadNum = 1;
    private boolean keepOrder;
    /**
     * in查询的参数文件，-表示从标准输入读取
     */
    private String inFile = StringConstant.STDIN;
    /**
     * in查询的分批大小
     */
    private int batchSize = 100;
    /**
     * 范围查询的起始
     */
    private String rangeStart;
    /**
     * 范围查询的结束
     */
    private String rangeEnd;
    /**
     * 每次范围查询的跨度
     */
    private String rangeSpan;
    /**
     * 显示没有查询到数据的参数
     */
    private boolean reverse;
    /**
     * 数据收集器
     */
    private String collector = CollectorEnum.QUERY.getValue();
    /**
     * 数据打印器
     */
    private String printer = DataPrinterEnum.FILE.getValue();
    /**
     * 输出文件
     */
    private String outFile = StringConstant.STDOUT;
    /**
     * 调试输出
     */
    private boolean verbose;
}
