package com.fmer.tools.parallelsql.constants;

import lombok.Getter;

import java.util.Arrays;

/**
 * 内容类型枚举
 * @author fengmeng
 * @date 2021/1/9 23:57
 */
@Getter
public enum ContentTypeEnum {
    /**
     * csv格式
     */
    CSV("csv"),
    /**
     * json格式
     */
    JSON("json"),
    ;
    private String value;
    ContentTypeEnum(String value){
        this.value = value;
    }

    public static ContentTypeEnum getByValue(String value){
        return Arrays.stream(ContentTypeEnum.values())
                .filter(o -> o.getValue().equalsIgnoreCase(value))
                .findFirst().orElseThrow(() -> new RuntimeException("无法识别的contentType: " + value));
    }
}
