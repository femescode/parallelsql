package com.fmer.tools.parallelsql.jdbc;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

/**
 * 行数据
 * @author fengmeng
 * @date 2021/1/9 22:04
 */
@Data
@AllArgsConstructor
public class RowData {
    private Object[] tags;
    private Map<String, Object> columnDataMap;

    public RowData(Map<String, Object> columnDataMap){
        this.columnDataMap = columnDataMap;
    }
}
