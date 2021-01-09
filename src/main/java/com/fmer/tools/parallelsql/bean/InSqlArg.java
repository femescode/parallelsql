package com.fmer.tools.parallelsql.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * in查询的sql参数
 * @author fengmeng
 * @date 2021/1/9 16:17
 */
@Data
@AllArgsConstructor
public class InSqlArg extends SqlArg {
    private String[] headers;
    private List<String[]> list;

    @Override
    public Object[] getArgs() {
        return list.stream().flatMap(Arrays::stream).toArray();
    }

    public String getInSql(){
        String inSql = list.stream().map(o -> {
            if(headers.length == 1){
                return "?";
            }
            return Arrays.stream(o).map(obj -> "?").collect(Collectors.joining(",", "(", ")"));
        }).collect(Collectors.joining(","));
        return inSql;
    }

}
