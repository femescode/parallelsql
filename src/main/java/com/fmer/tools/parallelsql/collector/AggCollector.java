package com.fmer.tools.parallelsql.collector;

import com.fmer.tools.parallelsql.bean.CliArgs;
import com.fmer.tools.parallelsql.bean.SqlResult;
import com.fmer.tools.parallelsql.collector.func.*;
import com.fmer.tools.parallelsql.jdbc.ColumnMeta;
import com.fmer.tools.parallelsql.jdbc.RowData;
import com.fmer.tools.parallelsql.jdbc.TableData;
import com.fmer.tools.parallelsql.printer.DataPrinter;
import com.fmer.tools.parallelsql.printer.Progress;
import com.fmer.tools.parallelsql.utils.CliUtils;
import com.fmer.tools.parallelsql.utils.GsonUtils;
import com.fmer.tools.parallelsql.utils.SqlUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 聚合结果的数据收集器
 * @author fengmeng
 * @date 2021/1/9 16:36
 */
public class AggCollector extends SqlResultCollector {
    private Set<String> groupByFields;
    private Map<String, String> aliasMap;
    private Map<String, String> aggFuncMap;
    /**
     * 存放聚集后的数据
     */
    private Map<String, AggRowData> aggRowDataMap;
    /**
     * 异常列表
     */
    private List<SqlResult> errorSqlResultList;


    public AggCollector(CliArgs cliArgs, DataPrinter dataPrinter, Progress progress) {
        super(cliArgs, dataPrinter, progress);
        this.groupByFields = Sets.newHashSet();
        this.aliasMap = Maps.newHashMap();
        this.aggFuncMap = Maps.newHashMap();
        this.aggRowDataMap = Maps.newHashMap();
        this.errorSqlResultList = Lists.newArrayList();
    }

    public void setSql(String sql){
        try {
            Statements statements = CCJSqlParserUtil.parseStatements(sql);
            List<Statement> statementList = statements.getStatements();
            if (statementList.get(0) instanceof Select) {
                Select select = (Select)statementList.get(0);
                parseSelectColumn(sql, select.getSelectBody());
            }else{
                throw new RuntimeException("不支持的sql语句, sql: " + sql);
            }
        } catch (JSQLParserException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    private void parseSelectColumn(String sql, SelectBody selectBody){
        if(selectBody instanceof PlainSelect){
            PlainSelect plainSelect = (PlainSelect)selectBody;
            for(SelectItem selectItem : plainSelect.getSelectItems()){
                selectItem.accept(new SelectItemVisitor() {
                    @Override
                    public void visit(SelectExpressionItem selectExpressionItem) {
                        String filedName = getExpressionName(selectExpressionItem.getExpression());
                        String aliasName = filedName;
                        Alias alias = selectExpressionItem.getAlias();
                        if(alias != null){
                            aliasName = alias.getName();
                        }
                        aliasMap.put(SqlUtils.dealColumnName(filedName), SqlUtils.dealColumnName(aliasName));
                        if(selectExpressionItem.getExpression() instanceof Function){
                            Function func = (Function)selectExpressionItem.getExpression();
                            String funcName = func.getName().toLowerCase();
                            if(FuncUtils.isAggFunc(funcName)){
                                aggFuncMap.put(aliasName, funcName);
                            }
                        }
                    }

                    @Override
                    public void visit(AllTableColumns allTableColumns) {
                        throw new RuntimeException("不支持的sql语句, sql: " + sql);
                    }

                    @Override
                    public void visit(AllColumns allColumns) {
                        throw new RuntimeException("不支持的sql语句, sql: " + sql);
                    }
                });
            }
            if(plainSelect.getGroupBy() != null){
                plainSelect.getGroupBy().accept(groupByElement -> {
                    for(Expression expression : groupByElement.getGroupByExpressions()){
                        String fieldName = SqlUtils.dealColumnName(getExpressionName(expression));
                        if(!aliasMap.containsKey(fieldName)){
                            throw new RuntimeException("group by字段不在select字段中, sql: " + sql);
                        }
                        groupByFields.add(aliasMap.get(fieldName));
                    }
                });
            }
        }else if(selectBody instanceof SetOperationList){
            SetOperationList setOpList = (SetOperationList)selectBody;
            SelectBody opSelectBody = setOpList.getSelects().get(0);
            parseSelectColumn(sql, opSelectBody);
        }else{
            throw new RuntimeException("不支持的sql语句, sql: " + sql);
        }
    }

    private String getExpressionName(Expression expression){
        if(expression instanceof Column){
            Column column = (Column)expression;
            return column.getColumnName();
        }else{
            return expression.toString();
        }
    }

    @Override
    public synchronized void add(SqlResult sqlResult){
        if(sqlResult.getE() != null){
            errorSqlResultList.add(sqlResult);
            return;
        }
        for(RowData rowData : sqlResult.getTableData().getRows()){
            if(!rowData.getColumnDataMap().keySet().containsAll(groupByFields)){
                throw new RuntimeException("行数据中没有包含group by字段，无法聚集计算! groupByFields: " + groupByFields + ", rowData: " + rowData.getColumnDataMap());
            }
            //找到聚集行
            String groupDataKey = groupByFields.stream().map(k -> CliUtils.getColumnString(rowData.getColumnDataMap().get(k))).collect(Collectors.joining("_"));
            AggRowData aggRowData = aggRowDataMap.computeIfAbsent(groupDataKey, k -> {
                Map<String, Object> groupByColumnDataMap = groupByFields.stream().collect(Collectors.toMap(key->key, key -> rowData.getColumnDataMap().get(key)));
                Map<String, AggFunc> newAggFuncMap = Maps.newHashMap();
                aggFuncMap.forEach((key, value) -> {
                    newAggFuncMap.put(key, FuncUtils.getAggFunc(value));
                });
                Object[] tags;
                if(MapUtils.isNotEmpty(groupByColumnDataMap)){
                    tags = groupByColumnDataMap.values().toArray();
                }else{
                    tags = new String[]{"-"};
                }
                return new AggRowData(tags, groupByColumnDataMap, newAggFuncMap);
            });
            //往聚集行里面添加数据
            Map<String, Object> aggDataMap = aggFuncMap.keySet().stream().collect(Collectors.toMap(key->key, key -> rowData.getColumnDataMap().get(key)));
            aggRowData.addData(aggDataMap);
        }
    }

    @Override
    public synchronized void finish(){
        //输出聚集行到打印器
        TableData tableData = new TableData();
        List<RowData> rows = Lists.newArrayList();
        for(AggRowData aggRowData : aggRowDataMap.values()){
            Map<String, Object> columnDataMap = Maps.newHashMap();
            columnDataMap.putAll(aggRowData.getColumnDataMap());
            aggRowData.getAggFuncDataMap().forEach((k, v) -> {
                columnDataMap.put(k, v.getResult());
            });
            RowData row = new RowData(aggRowData.getTags(), columnDataMap);
            rows.add(row);
        }
        tableData.setRows(rows);
        Map<String, ColumnMeta> fieldMap = Maps.newLinkedHashMap();
        groupByFields.forEach(k -> {
            fieldMap.put(k, new ColumnMeta());
        });
        aggFuncMap.forEach((k, v) -> {
            fieldMap.put(k, new ColumnMeta());
        });
        tableData.setFieldMap(fieldMap);
        SqlResult sqlResult = new SqlResult(cliArgs, null, null, tableData);
        super.add(sqlResult);
        //输出异常
        errorSqlResultList.forEach(super::add);

        super.finish();
    }
}
