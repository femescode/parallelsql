package com.fmer.tools.parallelsql.operation.bean;

import lombok.Data;

@Data
public class FieldInfo {
    private String source;
    private String schema;
    private String table;
    private String name;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldInfo fieldInfo = (FieldInfo) o;

        if (!source.equals(fieldInfo.source)) return false;
        if (!schema.equals(fieldInfo.schema)) return false;
        if (!table.equals(fieldInfo.table)) return false;
        return name.equals(fieldInfo.name);
    }

    @Override
    public int hashCode() {
        int result = source.hashCode();
        result = 31 * result + schema.hashCode();
        result = 31 * result + table.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }
}
