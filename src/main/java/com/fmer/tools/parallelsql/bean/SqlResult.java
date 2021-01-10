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
    private CliArgs cliArgs;
    private SqlArg sqlArg;
    private String sql;
    private Throwable e;
    private TableData tableData;

    public SqlResult(CliArgs cliArgs, SqlArg sqlArg, String sql, TableData tableData) {
        this.cliArgs = cliArgs;
        this.sqlArg = sqlArg;
        this.sql = sql;
        this.tableData = tableData;
    }

    public SqlResult(CliArgs cliArgs, SqlArg sqlArg, String sql, Throwable e) {
        this.cliArgs = cliArgs;
        this.sqlArg = sqlArg;
        this.sql = sql;
        this.e = e;
    }

}
