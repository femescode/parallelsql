package com.fmer.tools.parallelsql.bean;

import lombok.Data;

/**
 * sql参数基类
 * @author fengmeng
 * @date 2021/1/9 16:17
 */
@Data
public abstract class SqlArg {
    abstract public Object[] getArgs();
}
