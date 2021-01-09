package com.fmer.tools.parallelsql.constants;

import lombok.Getter;

import java.util.Arrays;
import java.util.Optional;

/**
 * 收集器类型
 * @author fengmeng
 * @date 2021/1/9 17:39
 */
@Getter
public enum CollectorEnum {
    /**
     * 查询
     */
    QUERY("query"),
    /**
     * 聚合
     */
    AGG("agg"),
    ;

    private String value;
    CollectorEnum(String value){
        this.value = value;
    }

    public static CollectorEnum getByValue(String value){
        return Arrays.stream(CollectorEnum.values())
                .filter(o -> o.getValue().equalsIgnoreCase(value))
                .findFirst().orElseThrow(() -> new RuntimeException("无法识别的collector: " + value));
    }
}
