package com.fmer.tools.parallelsql.jdbc;

import lombok.Data;

/**
 * 字段对象
 * @author fengmeng
 * @date 2021/1/9 20:53
 */
@Data
public class ColumnMeta {
    private String name;
    private int columnType;
    private String columnTypeName;
    private String columnClassName;
    private int columnDisplaySize;
    private int location;

    @Override
    public String toString() {
        return name + " " + columnTypeName + "[" + columnDisplaySize + "]";
    }
}
