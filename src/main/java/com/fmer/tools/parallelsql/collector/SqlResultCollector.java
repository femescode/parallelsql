package com.fmer.tools.parallelsql.collector;

import com.fmer.tools.parallelsql.bean.*;
import com.fmer.tools.parallelsql.jdbc.RowData;
import com.fmer.tools.parallelsql.jdbc.TableData;
import com.fmer.tools.parallelsql.printer.DataPrinter;
import com.fmer.tools.parallelsql.printer.Progress;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collections;
import java.util.List;

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
        }else{
            if(cliArgs.isReverse()){
                SqlArg sqlArg = sqlResult.getSqlArg();
                if(sqlArg instanceof InSqlArg){
                    InSqlArg inSqlArg = (InSqlArg)sqlArg;
                    List<RowData> rows = Lists.newArrayListWithCapacity(inSqlArg.getList().size());
                    for(String[] argOne : inSqlArg.getList()){
                        RowData rowData = new RowData(argOne, Collections.emptyMap());
                        rows.add(rowData);
                    }
                    TableData tableData = new TableData();
                    tableData.setRows(rows);
                    dataPrinter.print(new SqlResult(cliArgs, sqlArg, sqlResult.getSql(), tableData));
                }else if(sqlArg instanceof RangeSqlArg){
                    RangeSqlArg rangeSqlArg = (RangeSqlArg)sqlArg;
                    TableData tableData = new TableData();
                    tableData.setRows(Collections.singletonList(new RowData(rangeSqlArg.getArgs(), Collections.emptyMap())));
                    dataPrinter.print(new SqlResult(cliArgs, sqlArg, sqlResult.getSql(), tableData));
                }
            }
        }
    }

    public void finish(){
        dataPrinter.close();
        progress.printDoneProgress();
    }
}
