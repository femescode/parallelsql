package com.fmer.tools.parallelsql.jdbc;

import com.google.common.collect.Maps;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * 表格数据
 * @author fengmeng
 * @date 2021/1/9 20:56
 */
@Data
public class TableData {
    public Map<String, ColumnMeta> fieldMap = Maps.newHashMap();
    public List<RowData> rows;
}
