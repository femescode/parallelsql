package com.fmer.tools.parallelsql.operation;

import com.fmer.tools.parallelsql.operation.bean.DataGrid;
import com.fmer.tools.parallelsql.operation.bean.DataRow;
import com.fmer.tools.parallelsql.operation.bean.QueryParam;

import java.util.Iterator;

public class SqlQueryOperation extends BaseOperation{

    @Override
    public DataFetcher getFetcher(QueryParam param) {
        return new SqlQueryFetcher(param.getDataRowIt());
    }

    public static class SqlQueryFetcher implements DataFetcher{
        private Iterator<DataRow> sqlParamsIt;

        public SqlQueryFetcher(Iterator<DataRow> sqlParamsIt) {
            this.sqlParamsIt = sqlParamsIt;
        }

        @Override
        public DataGrid fetchNext() {
            if(!sqlParamsIt.hasNext()){
                return null;
            }
            DataRow sqlParam = sqlParamsIt.next();
            String sql = getSql(sqlParam);
            Object[] sqlParamArr = getSqlParamArr(sqlParam);
            return null;
        }
    }

    private static String getSql(DataRow sqlParam){
        //TODO
        return "";
    }

    private static Object[] getSqlParamArr(DataRow sqlParam){
        //TODO
        return new Object[0];
    }
}
