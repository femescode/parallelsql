package com.fmer.tools.parallelsql.constants;

import lombok.Getter;

import java.util.Arrays;

/**
 * 数据打印器枚举
 * @author fengmeng
 * @date 2021/1/9 17:41
 */
@Getter
public enum DataPrinterEnum {
    /**
     * 打印到控制台
     */
    CONSOLE("console"),
    /**
     * 打印到文件
     */
    FILE("file"),
    ;
    private String value;
    DataPrinterEnum(String value){
        this.value = value;
    }

    public static DataPrinterEnum getByValue(String value){
        return Arrays.stream(DataPrinterEnum.values())
                .filter(o -> o.getValue().equalsIgnoreCase(value))
                .findFirst().orElseThrow(() -> new RuntimeException("无法识别的printer: " + value));
    }
}
