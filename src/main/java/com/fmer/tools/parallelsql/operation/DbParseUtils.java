package com.fmer.tools.parallelsql.operation;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;

public class DbParseUtils {
    public static Map<Object, Set<String>> getNodeDbMap(SelectBody selectBody){
        Map<Object, Set<String>> nodeDbMap = Maps.newHashMap();
        getDbSetAndSetNodeDbMap(nodeDbMap, Maps.newHashMap(), selectBody);
        return nodeDbMap;
    }
    private static Set<String> getDbSetAndSetNodeDbMap(Map<Object, Set<String>> nodeDbMap, Map<String, String> tableDbMap, SelectItem selectItem){
        Set<String> dbSet = Sets.newHashSet();
        selectItem.accept(new SelectItemVisitor() {
            @Override
            public void visit(AllColumns allColumns) {

            }

            @Override
            public void visit(AllTableColumns allTableColumns) {
                dbSet.add(allTableColumns.getTable().getSchemaName());
            }

            @Override
            public void visit(SelectExpressionItem selectExpressionItem) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, selectExpressionItem));
            }
        });
        nodeDbMap.put(selectItem, dbSet);
        return dbSet;
    }

    private static Set<String> getDbSetAndSetNodeDbMap(Map<Object, Set<String>> nodeDbMap, Map<String, String> tableDbMap, FromItem fromItem){
        Set<String> dbSet = Sets.newHashSet();
        fromItem.accept(new FromItemVisitor() {
            @Override
            public void visit(Table table) {
                dbSet.add(table.getSchemaName());
                if(!tableDbMap.containsKey(table.getName())){
                    tableDbMap.put(table.getName(), table.getSchemaName());
                }
                if(table.getAlias() != null){
                    tableDbMap.put(table.getAlias().getName(), table.getSchemaName());
                }
            }

            @Override
            public void visit(SubSelect subSelect) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, subSelect.getSelectBody()));
            }

            @Override
            public void visit(SubJoin subJoin) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, subJoin.getLeft()));
                for(Join join : subJoin.getJoinList()){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, join));
                }
            }

            @Override
            public void visit(LateralSubSelect lateralSubSelect) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, (Expression) lateralSubSelect.getSubSelect()));
            }

            @Override
            public void visit(ValuesList valuesList) {

            }

            @Override
            public void visit(TableFunction tableFunction) {

            }

            @Override
            public void visit(ParenthesisFromItem parenthesisFromItem) {

            }
        });
        nodeDbMap.put(fromItem, dbSet);
        return dbSet;
    }
    private static Set<String> getDbSetAndSetNodeDbMap(Map<Object, Set<String>> nodeDbMap, Map<String, String> tableDbMap, Join join){
        Set<String> dbSet = Sets.newHashSet();
        dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, join.getRightItem()));
        dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, join.getOnExpression()));
        nodeDbMap.put(join, dbSet);
        return dbSet;
    }
    private static Set<String> getDbSetAndSetNodeDbMap(Map<Object, Set<String>> nodeDbMap, Map<String, String> tableDbMap, ItemsList itemsList){
        Set<String> dbSet = Sets.newHashSet();
        itemsList.accept(new ItemsListVisitor() {
            @Override
            public void visit(SubSelect subSelect) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, subSelect.getSelectBody()));
            }

            @Override
            public void visit(ExpressionList expressionList) {
                for(Expression expression : expressionList.getExpressions()){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, expression));
                }
            }

            @Override
            public void visit(NamedExpressionList namedExpressionList) {
                for(Expression expression : namedExpressionList.getExpressions()){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, expression));
                }
            }

            @Override
            public void visit(MultiExpressionList multiExpressionList) {
                for(ExpressionList expressionList : multiExpressionList.getExprList()){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, expressionList));
                }
            }
        });
        nodeDbMap.put(itemsList, dbSet);
        return dbSet;
    }
    private static Set<String> getDbSetAndSetNodeDbMap(Map<Object, Set<String>> nodeDbMap, Map<String, String> tableDbMap, Expression expression){
        Set<String> dbSet = Sets.newHashSet();
        expression.accept(new ExpressionVisitor() {
            @Override
            public void visit(BitwiseRightShift bitwiseRightShift) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, bitwiseRightShift.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, bitwiseRightShift.getRightExpression()));
            }

            @Override
            public void visit(BitwiseLeftShift bitwiseLeftShift) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, bitwiseLeftShift.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, bitwiseLeftShift.getRightExpression()));
            }

            @Override
            public void visit(NullValue nullValue) {

            }

            @Override
            public void visit(Function function) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, function.getParameters()));
            }

            @Override
            public void visit(SignedExpression signedExpression) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, signedExpression.getExpression()));
            }

            @Override
            public void visit(JdbcParameter jdbcParameter) {

            }

            @Override
            public void visit(JdbcNamedParameter jdbcNamedParameter) {

            }

            @Override
            public void visit(DoubleValue doubleValue) {

            }

            @Override
            public void visit(LongValue longValue) {

            }

            @Override
            public void visit(HexValue hexValue) {

            }

            @Override
            public void visit(DateValue dateValue) {

            }

            @Override
            public void visit(TimeValue timeValue) {

            }

            @Override
            public void visit(TimestampValue timestampValue) {

            }

            @Override
            public void visit(Parenthesis parenthesis) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, parenthesis.getExpression()));
            }

            @Override
            public void visit(StringValue stringValue) {

            }

            @Override
            public void visit(Addition addition) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, addition.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, addition.getRightExpression()));
            }

            @Override
            public void visit(Division division) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, division.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, division.getRightExpression()));
            }

            @Override
            public void visit(IntegerDivision integerDivision) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, integerDivision.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, integerDivision.getRightExpression()));
            }

            @Override
            public void visit(Multiplication multiplication) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, multiplication.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, multiplication.getRightExpression()));
            }

            @Override
            public void visit(Subtraction subtraction) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, subtraction.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, subtraction.getRightExpression()));
            }

            @Override
            public void visit(AndExpression andExpression) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, andExpression.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, andExpression.getRightExpression()));
            }

            @Override
            public void visit(OrExpression orExpression) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, orExpression.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, orExpression.getRightExpression()));
            }

            @Override
            public void visit(Between between) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, between.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, between.getBetweenExpressionStart()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, between.getBetweenExpressionEnd()));
            }

            @Override
            public void visit(EqualsTo equalsTo) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, equalsTo.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, equalsTo.getRightExpression()));
            }

            @Override
            public void visit(GreaterThan greaterThan) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, greaterThan.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, greaterThan.getRightExpression()));
            }

            @Override
            public void visit(GreaterThanEquals greaterThanEquals) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, greaterThanEquals.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, greaterThanEquals.getRightExpression()));
            }

            @Override
            public void visit(InExpression inExpression) {
                if(inExpression.getLeftExpression() != null){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, inExpression.getLeftExpression()));
                }
                if(inExpression.getRightExpression() != null){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, inExpression.getRightExpression()));
                }
                if(inExpression.getRightItemsList() != null){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, inExpression.getRightItemsList()));
                }
                if(inExpression.getLeftItemsList() != null){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, inExpression.getLeftItemsList()));
                }
                if(inExpression.getMultiExpressionList() != null){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, inExpression.getMultiExpressionList()));
                }
            }

            @Override
            public void visit(FullTextSearch fullTextSearch) {

            }

            @Override
            public void visit(IsNullExpression isNullExpression) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, isNullExpression.getLeftExpression()));
            }

            @Override
            public void visit(IsBooleanExpression isBooleanExpression) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, isBooleanExpression.getLeftExpression()));
            }

            @Override
            public void visit(LikeExpression likeExpression) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, likeExpression.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, likeExpression.getRightExpression()));
            }

            @Override
            public void visit(MinorThan minorThan) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, minorThan.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, minorThan.getRightExpression()));
            }

            @Override
            public void visit(MinorThanEquals minorThanEquals) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, minorThanEquals.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, minorThanEquals.getRightExpression()));
            }

            @Override
            public void visit(NotEqualsTo notEqualsTo) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, notEqualsTo.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, notEqualsTo.getRightExpression()));
            }

            @Override
            public void visit(Column column) {
                if(StringUtils.isNotEmpty(column.getTable().getSchemaName())){
                    dbSet.add(column.getTable().getSchemaName());
                }else{
                    String tableName = column.getTable().getName();
                    String schemaName = tableDbMap.get(tableName);
                    dbSet.add(schemaName);
                }
            }

            @Override
            public void visit(SubSelect subSelect) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, subSelect.getSelectBody()));
            }

            @Override
            public void visit(CaseExpression caseExpression) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, caseExpression.getSwitchExpression()));
                for(WhenClause whenClause : caseExpression.getWhenClauses()){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, whenClause));
                }
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, caseExpression.getElseExpression()));
            }

            @Override
            public void visit(WhenClause whenClause) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, whenClause.getWhenExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, whenClause.getThenExpression()));
            }

            @Override
            public void visit(ExistsExpression existsExpression) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, existsExpression.getRightExpression()));
            }

            @Override
            public void visit(AllComparisonExpression allComparisonExpression) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, (Expression) allComparisonExpression.getSubSelect()));
            }

            @Override
            public void visit(AnyComparisonExpression anyComparisonExpression) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, (Expression) anyComparisonExpression.getSubSelect()));
            }

            @Override
            public void visit(Concat concat) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, concat.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, concat.getRightExpression()));
            }

            @Override
            public void visit(Matches matches) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, matches.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, matches.getRightExpression()));
            }

            @Override
            public void visit(BitwiseAnd bitwiseAnd) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, bitwiseAnd.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, bitwiseAnd.getRightExpression()));
            }

            @Override
            public void visit(BitwiseOr bitwiseOr) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, bitwiseOr.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, bitwiseOr.getRightExpression()));
            }

            @Override
            public void visit(BitwiseXor bitwiseXor) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, bitwiseXor.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, bitwiseXor.getRightExpression()));
            }

            @Override
            public void visit(CastExpression castExpression) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, castExpression.getLeftExpression()));
            }

            @Override
            public void visit(Modulo modulo) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, modulo.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, modulo.getRightExpression()));
            }

            @Override
            public void visit(AnalyticExpression analyticExpression) {

            }

            @Override
            public void visit(ExtractExpression extractExpression) {

            }

            @Override
            public void visit(IntervalExpression intervalExpression) {

            }

            @Override
            public void visit(OracleHierarchicalExpression oracleHierarchicalExpression) {

            }

            @Override
            public void visit(RegExpMatchOperator regExpMatchOperator) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, regExpMatchOperator.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, regExpMatchOperator.getRightExpression()));
            }

            @Override
            public void visit(JsonExpression jsonExpression) {
                dbSet.add(jsonExpression.getColumn().getTable().getSchemaName());
            }

            @Override
            public void visit(JsonOperator jsonOperator) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, jsonOperator.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, jsonOperator.getRightExpression()));
            }

            @Override
            public void visit(RegExpMySQLOperator regExpMySQLOperator) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, regExpMySQLOperator.getLeftExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, regExpMySQLOperator.getRightExpression()));
            }

            @Override
            public void visit(UserVariable userVariable) {

            }

            @Override
            public void visit(NumericBind numericBind) {

            }

            @Override
            public void visit(KeepExpression keepExpression) {

            }

            @Override
            public void visit(MySQLGroupConcat mySQLGroupConcat) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, mySQLGroupConcat.getExpressionList()));
                for(OrderByElement orderByElement : mySQLGroupConcat.getOrderByElements()){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, orderByElement));
                }
            }

            @Override
            public void visit(ValueListExpression valueListExpression) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, valueListExpression.getExpressionList()));
            }

            @Override
            public void visit(RowConstructor rowConstructor) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, rowConstructor.getExprList()));
            }

            @Override
            public void visit(OracleHint oracleHint) {

            }

            @Override
            public void visit(TimeKeyExpression timeKeyExpression) {

            }

            @Override
            public void visit(DateTimeLiteralExpression dateTimeLiteralExpression) {

            }

            @Override
            public void visit(NotExpression notExpression) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, notExpression.getExpression()));
            }

            @Override
            public void visit(NextValExpression nextValExpression) {

            }

            @Override
            public void visit(CollateExpression collateExpression) {

            }

            @Override
            public void visit(SimilarToExpression similarToExpression) {

            }

            @Override
            public void visit(ArrayExpression arrayExpression) {
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, arrayExpression.getObjExpression()));
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, arrayExpression.getIndexExpression()));
            }
        });
        nodeDbMap.put(expression, dbSet);
        return dbSet;
    }
    private static Set<String> getDbSetAndSetNodeDbMap(Map<Object, Set<String>> nodeDbMap, Map<String, String> tableDbMap, GroupByElement groupByElement){
        Set<String> dbSet = Sets.newHashSet();
        for(Expression expression : groupByElement.getGroupByExpressions()){
            dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, expression));
        }
        nodeDbMap.put(groupByElement, dbSet);
        return dbSet;
    }
    private static Set<String> getDbSetAndSetNodeDbMap(Map<Object, Set<String>> nodeDbMap, Map<String, String> tableDbMap, OrderByElement orderByElement){
        Set<String> dbSet = Sets.newHashSet();
        dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, orderByElement.getExpression()));
        nodeDbMap.put(orderByElement, dbSet);
        return dbSet;
    }

    private static Set<String> getDbSetAndSetNodeDbMap(Map<Object, Set<String>> nodeDbMap, Map<String, String> tableDbMap, SelectBody selectBody){
        Set<String> dbSet = Sets.newHashSet();
        selectBody.accept(new SelectVisitor() {
            @Override
            public void visit(PlainSelect plainSelect) {
                for(SelectItem selectItem : plainSelect.getSelectItems()){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, selectItem));
                }
                dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, plainSelect.getFromItem()));
                if(CollectionUtils.isNotEmpty(plainSelect.getJoins())){
                    for(Join join : plainSelect.getJoins()){
                        dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, join));
                    }
                }
                if(plainSelect.getWhere() != null){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, plainSelect.getWhere()));
                }
                if(plainSelect.getGroupBy() != null){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, plainSelect.getGroupBy()));
                }
                if(plainSelect.getHaving() != null){
                    dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, plainSelect.getHaving()));
                }
                if(CollectionUtils.isNotEmpty(plainSelect.getOrderByElements())){
                    for(OrderByElement orderByElement : plainSelect.getOrderByElements()){
                        dbSet.addAll(getDbSetAndSetNodeDbMap(nodeDbMap, tableDbMap, orderByElement));
                    }
                }
            }

            @Override
            public void visit(SetOperationList setOperationList) {

            }

            @Override
            public void visit(WithItem withItem) {

            }

            @Override
            public void visit(ValuesStatement valuesStatement) {

            }
        });
        nodeDbMap.put(selectBody, dbSet);
        return dbSet;
    }
}
