package com.fmer.tools.parallelsql.operation.bean;

import com.google.common.collect.Maps;
import lombok.Data;

import java.util.Map;

@Data
public class DataRow {
    private Map<FieldInfo, Object> cols = Maps.newHashMap();
}
