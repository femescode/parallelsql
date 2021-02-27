package com.fmer.tools.parallelsql.operation;

import com.google.common.collect.Lists;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.apache.commons.collections.CollectionUtils;

public class PlainSelectUtils {
    public static boolean isEmpty(PlainSelect plainSelect){
        return CollectionUtils.isEmpty(plainSelect.getSelectItems())
                && plainSelect.getFromItem() == null
                && CollectionUtils.isEmpty(plainSelect.getJoins())
                && plainSelect.getWhere() == null
                && plainSelect.getGroupBy() == null
                && plainSelect.getHaving() == null
                && CollectionUtils.isEmpty(plainSelect.getOrderByElements())
                && plainSelect.getOffset() == null
                && plainSelect.getLimit() == null;
    }
    public static PlainSelect copy(PlainSelect plainSelect){
        PlainSelect newSelect = new PlainSelect();
        if(plainSelect.getSelectItems() != null){
            newSelect.setSelectItems(Lists.newArrayList(plainSelect.getSelectItems()));
        }
        newSelect.setFromItem(plainSelect.getFromItem());
        if(plainSelect.getJoins() != null){
            newSelect.setJoins(Lists.newArrayList(plainSelect.getJoins()));
        }
        newSelect.setWhere(plainSelect.getWhere());
        newSelect.setGroupByElement(plainSelect.getGroupBy());
        if(plainSelect.getOrderByElements() != null){
            newSelect.setOrderByElements(Lists.newArrayList(plainSelect.getOrderByElements()));
        }
        newSelect.setOffset(plainSelect.getOffset());
        newSelect.setLimit(plainSelect.getLimit());
        return newSelect;
    }
}
