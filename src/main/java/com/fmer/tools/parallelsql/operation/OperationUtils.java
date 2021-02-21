package com.fmer.tools.parallelsql.operation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class OperationUtils {
    private OperationUtils(){}

    public static Operation parseSql(String sql){
        try {
            Statements statements = CCJSqlParserUtil.parseStatements(sql);
            List<Statement> statementList = statements.getStatements();
            if(statementList.size() > 1){
                throw new RuntimeException("只支持一条sql语句, sql: " + sql);
            }
            Assert.isInstanceOf(Select.class, statementList.get(0), "只支持select语句, sql: " + sql);
            Select select = (Select)statementList.get(0);
            SelectBody selectBody = select.getSelectBody();
        } catch (JSQLParserException e) {
            ExceptionUtils.rethrow(e);
        }
        return null;
    }

    private static Set<String> getSchemas(FromItem fromItem){
        return Sets.newHashSet(((Table)fromItem).getSchemaName());
    }

    private static void addFromItem(SqlQueryOperation sqlQueryOperation, FromItem fromItem){
        sqlQueryOperation.getPlainSelect().setFromItem(fromItem);
    }

    private static void addJoin(SqlQueryOperation sqlQueryOperation, Join join){
        if(sqlQueryOperation.getPlainSelect().getJoins() == null){
            sqlQueryOperation.getPlainSelect().setJoins(Lists.newArrayList());
        }
        sqlQueryOperation.getPlainSelect().getJoins().add(join);
    }



    private static BaseOperation parseSelectBody(SelectBody selectBody){
        selectBody.accept(new SelectVisitor() {
            @Override
            public void visit(PlainSelect plainSelect) {
                plainSelect.getSelectItems();
                plainSelect.getFromItem();
                plainSelect.getJoins();
                plainSelect.getWhere();
                plainSelect.getGroupBy();
                plainSelect.getHaving();
                plainSelect.getOrderByElements();
                plainSelect.getOffset();
                plainSelect.getLimit();

                BaseOperation root;
                Map<Table, SqlQueryOperation> sqlQueryOperationMap = Maps.newHashMap();
                //处理from
                SqlQueryOperation fromOperation = new SqlQueryOperation();
                Set<String> fromSchemas = getSchemas(plainSelect.getFromItem());
                addFromItem(fromOperation, plainSelect.getFromItem());
                root = fromOperation;
                sqlQueryOperationMap.put((Table) plainSelect.getFromItem(), fromOperation);


                //处理join
                for(Join join : plainSelect.getJoins()){
                    Set<String> schemas = getSchemas(join.getRightItem());
                    if(fromSchemas.containsAll(schemas)){
                        addJoin(fromOperation, join);
                    }else{
                        SqlQueryOperation joinOperation = new SqlQueryOperation();
                        addJoin(joinOperation, join);
                        sqlQueryOperationMap.put(((Table)join.getRightItem()), joinOperation);
                        root = new JoinOperation(root, joinOperation);
                    }
                }
                //TODO 像 or条件、join不同库、in查询不同库、exists查询不同库、子查询不同库，都肯定是要添加operation层级的

                //处理where
                Expression where = plainSelect.getWhere();
                EqualsTo equalsTo = (EqualsTo)where;
                Column left = (Column) equalsTo.getLeftExpression();
                if(sqlQueryOperationMap.containsKey(left.getTable())){
                    sqlQueryOperationMap.get(left.getTable());
                    // 将过滤条件添加到相关表的sqlQueryOperation TODO
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
        return null;
    }
}
