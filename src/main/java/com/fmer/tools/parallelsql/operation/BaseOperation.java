package com.fmer.tools.parallelsql.operation;

import com.fmer.tools.parallelsql.jdbc.TableData;
import com.fmer.tools.parallelsql.operation.bean.DataGrid;
import com.fmer.tools.parallelsql.operation.bean.DataRow;
import lombok.Data;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.apache.commons.collections.CollectionUtils;

import java.util.Set;

@Data
public abstract class BaseOperation implements Operation {
    public PlainSelect plainSelect = new PlainSelect();

    @Override
    public void initParam(DataRow param){}

    @Override
    public DataGrid fetchNext() {
        return null;
    }

    @Override
    public String toString() {
        if(PlainSelectUtils.isEmpty(plainSelect)){
            return this.getClass().getSimpleName();
        }else{
            if(plainSelect.getFromItem() == null && CollectionUtils.isNotEmpty(plainSelect.getJoins())){
                PlainSelect newSelect = PlainSelectUtils.copy(plainSelect);
                Join join = newSelect.getJoins().remove(0);
                newSelect.setFromItem(join.getRightItem());
                if(newSelect.getWhere() == null){
                    newSelect.setWhere(join.getOnExpression());
                }else{
                    newSelect.setWhere(new AndExpression(newSelect.getWhere(), join.getOnExpression()));
                }
                return this.getClass().getSimpleName() + ": " + newSelect.toString();
            }else{
                return this.getClass().getSimpleName() + ": " + plainSelect.toString();
            }
        }
    }
}
