package com.fmer.tools.parallelsql.utils;

import com.fmer.tools.parallelsql.jdbc.ColumnMeta;
import com.fmer.tools.parallelsql.jdbc.RowData;
import com.fmer.tools.parallelsql.jdbc.TableData;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * jdbc的resultSet工具类
 * @author fengmeng
 * @date 2021/1/9 21:18
 */
public class ResultSetUtils {
    private ResultSetUtils(){}

    public static TableData queryTableData(ResultSet rs) throws SQLException {
        TableData tableData = new TableData();
        ResultSetMetaData rsmd = rs.getMetaData();
        Map<String, ColumnMeta> fieldMap = Maps.newLinkedHashMapWithExpectedSize(rsmd.getColumnCount());
        for(int i=1;i <= rsmd.getColumnCount(); i++){
            ColumnMeta columnMeta = new ColumnMeta();
            columnMeta.setName(rsmd.getColumnLabel(i));
            columnMeta.setColumnType(rsmd.getColumnType(i));
            columnMeta.setColumnTypeName(rsmd.getColumnTypeName(i));
            columnMeta.setColumnClassName(rsmd.getColumnClassName(i));
            columnMeta.setColumnDisplaySize(rsmd.getColumnDisplaySize(i));
            columnMeta.setLocation(i);
            fieldMap.put(rsmd.getColumnLabel(i), columnMeta);
        }
        tableData.setFieldMap(fieldMap);

        //设置rows
        List<RowData> rows = getRows(rs);
        tableData.setRows(rows);
        return tableData;
    }

    private static List<RowData> getRows(ResultSet rs) throws SQLException{
        ResultSetMetaData rsmd = rs.getMetaData();
        List<RowData> rows = Lists.newArrayList();
        while(rs.next()){
            Map<String, Object> columnMap = Maps.newLinkedHashMapWithExpectedSize(rsmd.getColumnCount());
            for(int i=1;i <= rsmd.getColumnCount(); i++){
                String columnLabel = rsmd.getColumnLabel(i);
                columnMap.put(columnLabel, getColumnData(rsmd, rs, i));
            }
            rows.add(new RowData(columnMap));
        }
        return rows;
    }

    private static Object getColumnData(ResultSetMetaData rsmd, ResultSet rs, int i) throws SQLException{
        String columnTypeName = rsmd.getColumnTypeName(i);
        String columnClassName = rsmd.getColumnClassName(i);
        Object value = rs.getObject(i);

        Class<?> clazz;
        try {
            clazz = Class.forName(columnClassName);
        } catch (ClassNotFoundException e) {
            return ExceptionUtils.rethrow(e);
        }
        if(clazz.equals(Boolean.class) && "TINYINT".equalsIgnoreCase(columnTypeName)){
            value = rs.getShort(i);
        }
        return value;
    }
}
