package com.fmer.tools.parallelsql.operation;

import com.fmer.tools.parallelsql.operation.bean.DataGrid;
import com.fmer.tools.parallelsql.operation.bean.DataRow;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

public class SqlQueryOperation extends BaseOperation{
    private Iterator<DataRow> sqlParamsIt;

    @Override
    public void initParam(DataRow param){
        //TODO
        sqlParamsIt = Lists.newArrayList(param).iterator();
    }

    @Override
    public DataGrid fetchNext() {
        while (sqlParamsIt.hasNext()){
            DataRow sqlParam = sqlParamsIt.next();
            String sql = getSql(sqlParam);
            Object[] sqlParamArr = getSqlParamArr(sqlParam);

        }
        return null;
    }

    private String getSql(DataRow sqlParam){
        //TODO
        return "";
    }

    private Object[] getSqlParamArr(DataRow sqlParam){
        //TODO
        return new Object[0];
    }
}
