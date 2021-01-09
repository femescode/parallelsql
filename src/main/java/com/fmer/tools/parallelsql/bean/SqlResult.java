package com.fmer.tools.parallelsql.bean;

import com.fmer.tools.parallelsql.jdbc.TableData;
import lombok.Data;

/**
 * sql执行的结果
 * @author fengmeng
 * @date 2021/1/9 16:27
 */
@Data
public class SqlResult {
    private SqlArg sqlArg;
    private Throwable e;
    private TableData tableData;

    public SqlResult(SqlArg sqlArg, TableData tableData) {
        this.sqlArg = sqlArg;
        this.tableData = tableData;
    }

    public SqlResult(SqlArg sqlArg, Throwable e) {
        this.sqlArg = sqlArg;
        this.e = e;
    }

}
