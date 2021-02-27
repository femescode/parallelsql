package com.fmer.tools.parallelsql.operation.bean;

import com.google.common.collect.Lists;
import lombok.Data;

import java.util.Iterator;

@Data
public class QueryParam {
    /**
     * 查询类型，传入的参数0，或关联的数据行1
     */
    private String type;
    private DataRow dataRow;

    public QueryParam(String type, DataRow dataRow) {
        this.type = type;
        this.dataRow = dataRow;
    }

    public Iterator<DataRow> getDataRowIt(){
        return Lists.newArrayList(this.dataRow).iterator();
    }
}
