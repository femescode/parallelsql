package com.fmer.tools.parallelsql.collector;

import com.fmer.tools.parallelsql.bean.CliArgs;
import com.fmer.tools.parallelsql.bean.SqlResult;
import com.fmer.tools.parallelsql.printer.DataPrinter;

/**
 * 聚合结果的数据收集器
 * @author fengmeng
 * @date 2021/1/9 16:36
 */
public class AggCollector extends SqlResultCollector {

    public AggCollector(CliArgs cliArgs, DataPrinter dataPrinter) {
        super(cliArgs, dataPrinter);
    }

    @Override
    public void add(SqlResult sqlResult){

    }

    @Override
    public void finish(){

    }
}
