package com.fmer.tools.parallelsql.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 参数的位置，用于计算进度
 * @author fengmeng
 * @date 2021/1/10 10:46
 */
@Data
@AllArgsConstructor
public class ArgLocation {
    private long offset;
    private long size;
}
