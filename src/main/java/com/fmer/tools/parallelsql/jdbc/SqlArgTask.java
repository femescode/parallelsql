package com.fmer.tools.parallelsql.jdbc;

import com.fmer.tools.parallelsql.bean.CliArgs;
import com.fmer.tools.parallelsql.bean.InSqlArg;
import com.fmer.tools.parallelsql.bean.SqlArg;
import com.fmer.tools.parallelsql.bean.SqlResult;
import com.fmer.tools.parallelsql.printer.Progress;
import com.fmer.tools.parallelsql.utils.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * sql参数处理任务
 * @author fengmeng
 * @date 2021/1/9 20:10
 */
public class SqlArgTask implements Supplier<SqlResult> {
    private static final int MAX_RESULT_LEN = 240;
    private CliArgs cliArgs;
    private SqlArg sqlArg;
    private JdbcTemplate jdbcTemplate;
    private Progress progress;

    public SqlArgTask(CliArgs cliArgs, SqlArg sqlArg, JdbcTemplate jdbcTemplate, Progress progress){
        this.cliArgs = cliArgs;
        this.sqlArg = sqlArg;
        this.jdbcTemplate = jdbcTemplate;
        this.progress = progress;
    }

    @Override
    public SqlResult get() {
        String sql = CliUtils.getExecutableSql(cliArgs.getSql(), sqlArg);
        String plainSql = SqlUtils.getPlainSql(sql, sqlArg.getArgs());
        if(cliArgs.isVerbose()){
            VerboseLogger.log(this.progress.getProgressToDisplay() + " sql: " + plainSql);
        }
        TableData tableData = null;
        long startTime = System.currentTimeMillis();
        try{
            tableData = jdbcTemplate.query(sql, ResultSetUtils::queryTableData, sqlArg.getArgs());
            //排序in查询的结果行，并设置每行的tag
            sortRowsIfKeepOrderAndSetTag(tableData);
            return new SqlResult(this.cliArgs, this.sqlArg, plainSql, tableData);
        }catch (Throwable e){
            return new SqlResult(this.cliArgs, this.sqlArg, plainSql, e);
        }finally {
            long cost = System.currentTimeMillis() - startTime;
            if(cliArgs.isVerbose()){
                String result = String.format("cost: %dms", cost);
                if(tableData != null){
                    result += " Total: " + tableData.getRows().size() + " " + GsonUtils.getGson().toJson(tableData.getRows());
                }
                if(result.length() > MAX_RESULT_LEN){
                    result = StringUtils.abbreviate(result, MAX_RESULT_LEN);
                }
                VerboseLogger.log(this.progress.getProgressToDisplay() + " result: " + result);
            }
        }
    }

    /**
     * 排序in查询的结果行，如果开启了keepOrder参数
     * @param tableData
     */
    private void sortRowsIfKeepOrderAndSetTag(TableData tableData){
        if(cliArgs.isKeepOrder() && sqlArg instanceof InSqlArg){
            Map<String, ColumnMeta> fieldMap = tableData.getFieldMap();
            InSqlArg inSqlArg = (InSqlArg)sqlArg;
            //查询出来的字段包含了所有in字段，才可将in查询出来的结果排序
            if(fieldMap.keySet().containsAll(Arrays.asList(inSqlArg.getHeaders()))){
                Multimap<String, RowData> multimap = Multimaps.newMultimap(Maps.newHashMap(), ArrayList::new);
                for(RowData data : tableData.getRows()){
                    String key = Arrays.stream(inSqlArg.getHeaders()).map(k->String.valueOf(data.getColumnDataMap().get(k))).collect(Collectors.joining("_"));
                    multimap.put(key, data);
                }
                List<RowData> sortedRows = Lists.newArrayList();
                for(String[] keys : inSqlArg.getList()){
                    String key = String.join("_", keys);
                    Collection<RowData> datas = multimap.get(key);
                    datas.forEach(r -> r.setTags(keys));
                    if(CollectionUtils.isNotEmpty(datas)){
                        sortedRows.addAll(datas);
                    }
                }
                tableData.setRows(sortedRows);
            }else{
                if(CollectionUtils.isNotEmpty(tableData.getRows())){
                    for(RowData rowData : tableData.getRows()){
                        rowData.setTags(sqlArg.getArgs());
                    }
                }
            }
        }else{
            if(CollectionUtils.isNotEmpty(tableData.getRows())){
                for(RowData rowData : tableData.getRows()){
                    rowData.setTags(sqlArg.getArgs());
                }
            }
        }
    }

}
