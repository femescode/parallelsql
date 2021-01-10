package com.fmer.tools.parallelsql.collector.func;

import lombok.Data;

/**
 * sql聚集函数
 * @author fengmeng
 * @date 2021/1/10 13:27
 */
@Data
public abstract class AggFunc {
    /**
     * 往聚集函数中添加数据
     * @param value
     */
    public abstract void addValue(Object value);

    /**
     * 获取聚集函数的结果
     * @return
     */
    public abstract Object getResult();
}
