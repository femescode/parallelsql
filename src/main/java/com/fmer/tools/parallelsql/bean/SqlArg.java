package com.fmer.tools.parallelsql.bean;

import lombok.Data;

/**
 * sql参数基类
 * @author fengmeng
 * @date 2021/1/9 16:17
 */
@Data
public abstract class SqlArg {
    private ArgLocation argLocation;

    /**
     * 获取jdbc参数
     * @return
     */
    abstract public Object[] getArgs();
}
