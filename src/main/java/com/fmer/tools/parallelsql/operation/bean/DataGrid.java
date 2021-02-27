package com.fmer.tools.parallelsql.operation.bean;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Data;

import java.util.List;
import java.util.Set;

@Data
public class DataGrid {
    private Set<FieldInfo> fieldInfos = Sets.newHashSet();
    private List<DataRow> rows = Lists.newArrayList();
}
