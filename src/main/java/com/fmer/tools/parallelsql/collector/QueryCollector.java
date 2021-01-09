package com.fmer.tools.parallelsql.collector;

import com.fmer.tools.parallelsql.bean.CliArgs;
import com.fmer.tools.parallelsql.bean.SqlResult;
import com.fmer.tools.parallelsql.printer.DataPrinter;

/**
 * 普通查询的结果收集器
 * @author fengmeng
 * @date 2021/1/9 16:35
 */
public class QueryCollector extends SqlResultCollector {

    public QueryCollector(CliArgs cliArgs, DataPrinter dataPrinter) {
        super(cliArgs, dataPrinter);
    }

    @Override
    public void add(SqlResult sqlResult){
        super.add(sqlResult);
    }

    @Override
    public void finish(){
        dataPrinter.close();
    }
}
