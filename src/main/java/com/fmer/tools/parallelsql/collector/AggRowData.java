package com.fmer.tools.parallelsql.collector;

import com.fmer.tools.parallelsql.collector.func.AggFunc;
import lombok.Data;

import java.util.Map;

/**
 * 存放聚集函数的每一行数据
 * @author fengmeng
 * @date 2021/1/10 13:51
 */
@Data
public class AggRowData {
    private Object[] tags;
    private Map<String, Object> columnDataMap;
    private Map<String, AggFunc> aggFuncDataMap;
    public AggRowData(Object[] tags, Map<String, Object> columnDataMap, Map<String, AggFunc> aggFuncDataMap){
        this.tags = tags;
        this.columnDataMap = columnDataMap;
        this.aggFuncDataMap = aggFuncDataMap;
    }
    public void addData(Map<String, Object> aggDataMap){
        aggFuncDataMap.forEach((k, v) -> {
            Object data = aggDataMap.get(k);
            if(data != null){
                v.addValue(data);
            }
        });
    }
}
