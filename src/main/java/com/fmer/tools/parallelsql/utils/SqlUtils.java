package com.fmer.tools.parallelsql.utils;

import java.util.Arrays;

/**
 * sql工具类
 * @author fengmeng
 * @date 2021/1/9 21:20
 */
public class SqlUtils {
    private SqlUtils(){}

    public static String getPlainSql(String sql, Object... args){
        sql = sql + " ";
        String[] sqls = sql.split("\\?");
        if(sqls.length != args.length + 1){
            throw new RuntimeException("sql中的?与参数个数不匹配，sql: " + sql + ", args: " + Arrays.toString(args));
        }
        StringBuilder sb = new StringBuilder();
        sb.append(sqls[0]);
        for(int i=1; i < sqls.length; i++){
            sb.append("'").append(args[i-1]).append("'");
            sb.append(sqls[i]);
        }
        return sb.toString();
    }

    public static String dealColumnName(String columnName){
        return columnName.toLowerCase();
    }
}
