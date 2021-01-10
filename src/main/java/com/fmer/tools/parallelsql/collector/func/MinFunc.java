package com.fmer.tools.parallelsql.collector.func;

import com.fmer.tools.parallelsql.utils.CliUtils;
import lombok.Data;

import java.math.BigDecimal;

/**
 * 求最小值的函数
 * @author fengmeng
 * @date 2021/1/10 13:24
 */
@Data
public class MinFunc extends AggFunc{
    private BigDecimal value;
    @Override
    public void addValue(Object data) {
        if(value == null){
            value = CliUtils.getColumnBigDecimal(data);
        }else{
            BigDecimal newVal = CliUtils.getColumnBigDecimal(data);
            if(value.compareTo(newVal) > 0){
                value = newVal;
            }
        }
    }

    @Override
    public Object getResult() {
        return value;
    }
}
