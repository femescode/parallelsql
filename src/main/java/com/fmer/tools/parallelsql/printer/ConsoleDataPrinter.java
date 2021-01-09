package com.fmer.tools.parallelsql.printer;

import com.fmer.tools.parallelsql.bean.CliArgs;
import com.fmer.tools.parallelsql.bean.SqlResult;
import com.fmer.tools.parallelsql.jdbc.RowData;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据往控制台打印
 * @author fengmeng
 * @date 2021/1/9 16:38
 */
public class ConsoleDataPrinter extends DataPrinter {
    private CliArgs cliArgs;
    private AtomicInteger i;
    public ConsoleDataPrinter(CliArgs cliArgs){
        this.cliArgs = cliArgs;
        this.i = new AtomicInteger();
    }

    @Override
    public void print(SqlResult sqlResult){
        if(sqlResult.getE() != null){
            System.out.println(sqlResult.getSqlArg() + "\t执行发生异常, e: " + sqlResult.getE().getMessage());
            return;
        }
        if(CollectionUtils.isNotEmpty(sqlResult.getTableData().getRows())){
            for(RowData rowData : sqlResult.getTableData().getRows()){
                System.out.println(Arrays.toString(rowData.getTags()) + "\t" + new Gson().toJson(rowData.getColumnDataMap()));
            }
        }

        i.incrementAndGet();
    }

    @Override
    public void close(){
        System.out.flush();
    }
}
