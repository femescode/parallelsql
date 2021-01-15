package com.fmer.tools.parallelsql.collector;

import com.fmer.tools.parallelsql.bean.CliArgs;
import com.fmer.tools.parallelsql.bean.SqlResult;
import com.fmer.tools.parallelsql.printer.DataPrinter;
import com.fmer.tools.parallelsql.printer.Progress;
import org.apache.commons.collections.CollectionUtils;

/**
 * sql执行结果的收集器
 * @author fengmeng
 * @date 2021/1/9 16:32
 */
public abstract class SqlResultCollector {
    protected CliArgs cliArgs;
    protected DataPrinter dataPrinter;
    private Progress progress;


    public SqlResultCollector(CliArgs cliArgs, DataPrinter dataPrinter, Progress progress){
        this.cliArgs = cliArgs;
        this.dataPrinter = dataPrinter;
        this.progress = progress;
    }

    public void add(SqlResult sqlResult){
        if(CollectionUtils.isNotEmpty(sqlResult.getTableData().getRows())){
            dataPrinter.print(sqlResult);
        }
    }

    public void finish(){
        dataPrinter.close();
        progress.printDoneProgress();
    }
}
