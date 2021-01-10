package com.fmer.tools.parallelsql.collector.func;

import com.fmer.tools.parallelsql.utils.CliUtils;
import lombok.Data;

/**
 * group_concat函数
 * @author fengmeng
 * @date 2021/1/10 13:26
 */
@Data
public class GroupConcatFunc extends AggFunc{
    private StringBuilder sb = new StringBuilder();
    @Override
    public void addValue(Object value) {
        sb.append(",").append(CliUtils.getColumnString(value));
    }

    @Override
    public Object getResult() {
        return sb.toString();
    }
}
