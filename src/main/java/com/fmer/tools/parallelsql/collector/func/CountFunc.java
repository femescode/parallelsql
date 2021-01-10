package com.fmer.tools.parallelsql.collector.func;

import com.fmer.tools.parallelsql.utils.CliUtils;
import lombok.Data;

import java.math.BigDecimal;

/**
 * 求数量的函数
 * @author fengmeng
 * @date 2021/1/10 13:25
 */
@Data
public class CountFunc extends AggFunc{
    private BigDecimal value;
    @Override
    public void addValue(Object data) {
        if(value == null){
            value = CliUtils.getColumnBigDecimal(data);
        }else{
            value = value.add(CliUtils.getColumnBigDecimal(data));
        }
    }

    @Override
    public Object getResult() {
        return value;
    }
}
