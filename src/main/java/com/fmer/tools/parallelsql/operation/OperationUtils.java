package com.fmer.tools.parallelsql.operation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
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

    public static void main(String[] args) {
        Operation operation = OperationUtils.parseSql("select * from shop.order a " +
                "join tmall.order_item b on a.order_id=b.order_id " +
                "left join cold.waybill_item c on b.order_id=c.order_id and b.goods_id=c.goods_id " +
                "where a.add_time > ? and a.add_time < ? and b.goods_id in (1,2,3) and c.sn='12345'");
        System.out.println(operation);
    }

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
            return parseSelectBody(selectBody);
        } catch (JSQLParserException e) {
            return ExceptionUtils.rethrow(e);
        }
    }

    private static String getSchema(FromItem fromItem){
        return ((Table)fromItem).getSchemaName();
    }

    private static void addFromItem(BaseOperation sqlQueryOperation, FromItem fromItem){
        sqlQueryOperation.getPlainSelect().setFromItem(fromItem);
    }

    private static void addJoin(BaseOperation sqlQueryOperation, Join join){
        if(sqlQueryOperation.getPlainSelect().getJoins() == null){
            sqlQueryOperation.getPlainSelect().setJoins(Lists.newArrayList());
        }
        sqlQueryOperation.getPlainSelect().getJoins().add(join);
    }

    private static void addWhere(BaseOperation sqlQueryOperation, Expression where){
        if(sqlQueryOperation.getPlainSelect().getWhere() == null){
            sqlQueryOperation.getPlainSelect().setWhere(where);
        }else{
            sqlQueryOperation.getPlainSelect().setWhere(new AndExpression(sqlQueryOperation.getPlainSelect().getWhere(), where));
        }
    }

    private static void addField(BaseOperation sqlQueryOperation, SelectItem selectItem){
        if(sqlQueryOperation.getPlainSelect().getSelectItems() == null){
            sqlQueryOperation.getPlainSelect().setSelectItems(Lists.newArrayList());
        }
        sqlQueryOperation.getPlainSelect().getSelectItems().add(selectItem);
    }

    private static void addOperation(Map<String,SqlQueryOperation> sqlQueryOperationMap, String schemaName, SqlQueryOperation sqlQueryOperation){
        sqlQueryOperationMap.put(schemaName, sqlQueryOperation);
    }

    private static SqlQueryOperation getOperation(Map<String,SqlQueryOperation> sqlQueryOperationMap, String schemaName){
        return sqlQueryOperationMap.get(schemaName);
    }

    private static void parseWhereExpress(Expression where, Map<Object, Set<String>> exprDbMap, Map<String,SqlQueryOperation> sqlQueryOperationMap, FilterOperation filterOperation){
        Set<String> schemaSet = exprDbMap.get(where);
        if(schemaSet.size() == 1){
            //下推到from或join
            SqlQueryOperation operation = getOperation(sqlQueryOperationMap, schemaSet.iterator().next());
            addWhere(operation, where);
        }else if(where instanceof AndExpression){
            AndExpression andExpression = (AndExpression)where;
            Expression leftExpr = andExpression.getLeftExpression();
            parseWhereExpress(leftExpr, exprDbMap, sqlQueryOperationMap, filterOperation);
            Expression rightExpr = andExpression.getRightExpression();
            parseWhereExpress(rightExpr, exprDbMap, sqlQueryOperationMap, filterOperation);
        }else{
            //放到filterOperation中
            addWhere(filterOperation, where);
        }
    }

    private static BaseOperation parseSelectBody(SelectBody selectBody){
        AtomicReference<BaseOperation> rootRef = new AtomicReference<>();
        selectBody.accept(new SelectVisitor() {
            @Override
            public void visit(PlainSelect plainSelect) {
                //TODO 像 or条件、join不同库、in查询不同库、exists查询不同库、子查询不同库，都肯定是要添加operation层级的
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
                Map<String,SqlQueryOperation> sqlQueryOperationMap = Maps.newHashMap();
                //处理from
                SqlQueryOperation fromOperation = new SqlQueryOperation();
                String fromSchema = getSchema(plainSelect.getFromItem());
                addFromItem(fromOperation, plainSelect.getFromItem());
                root = fromOperation;
                addOperation(sqlQueryOperationMap, fromSchema, fromOperation);

                //处理join
                for(Join join : plainSelect.getJoins()){
                    String schema = getSchema(join.getRightItem());
                    SqlQueryOperation operation = getOperation(sqlQueryOperationMap, schema);
                    if(operation != null){
                        addJoin(operation, join);
                    }else{
                        SqlQueryOperation joinOperation = new SqlQueryOperation();
                        addJoin(joinOperation, join);
                        joinOperation.toString();
                        addOperation(sqlQueryOperationMap, schema, joinOperation);
                        root = new JoinOperation(root, joinOperation);
                    }
                }

                //解析sql中各表达式节点使用了哪些库
                Map<Object, Set<String>> nodeDbMap = DbParseUtils.getNodeDbMap(selectBody);

                //处理where
                Expression where = plainSelect.getWhere();
                if(where != null){
                    FilterOperation filterOperation = new FilterOperation(root);
                    parseWhereExpress(where, nodeDbMap, sqlQueryOperationMap, filterOperation);
                    if(filterOperation.getPlainSelect().getWhere() != null){
                        root = filterOperation;
                    }
                }

                //处理group by
                if(sqlQueryOperationMap.size() == 1){
                    //下推到其中 TODO
                }

                //处理having

                //处理order by

                //处理limit

                //处理fields
                FieldsOperation fieldsOperation = new FieldsOperation(root);
                for(SelectItem selectItem : plainSelect.getSelectItems()){
                    addField(fieldsOperation, selectItem);
                }
                root = fieldsOperation;

                rootRef.set(root);
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
        return rootRef.get();
    }
}
