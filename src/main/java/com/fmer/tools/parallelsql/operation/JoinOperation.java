package com.fmer.tools.parallelsql.operation;

import com.fmer.tools.parallelsql.operation.bean.DataGrid;
import com.fmer.tools.parallelsql.operation.bean.DataRow;
import com.fmer.tools.parallelsql.operation.bean.QueryParam;

public class JoinOperation extends BaseOperation {
    private boolean isLeftJoin;
    private BaseOperation from;
    private BaseOperation join;

    public JoinOperation(BaseOperation from, BaseOperation join) {
        this.from = from;
        this.join = join;
    }

    @Override
    public DataFetcher getFetcher(QueryParam param) {
        DataFetcher fromFetcher = this.from.getFetcher(param);
        return new JoinFetcher(fromFetcher, join);
    }

    public static class JoinFetcher implements DataFetcher{
        private DataFetcher fromFetcher;
        private BaseOperation join;

        public JoinFetcher(DataFetcher fromFetcher, BaseOperation join) {
            this.fromFetcher = fromFetcher;
            this.join = join;
        }

        @Override
        public DataGrid fetchNext() {
            DataGrid dataGrid = fromFetcher.fetchNext();
            if(dataGrid == null){
                return null;
            }
            DataGrid newDataGrid = new DataGrid();
            newDataGrid.getFieldInfos().addAll(dataGrid.getFieldInfos());
            for (DataRow dataRow : dataGrid.getRows()) {
                DataFetcher dataFetcher = join.getFetcher(new QueryParam("1", dataRow));
                DataGrid joinData = dataFetcher.fetchNext();
                if (joinData != null) {
                    newDataGrid.getFieldInfos().addAll(joinData.getFieldInfos());
                    for(DataRow joinDataRow : joinData.getRows()){
                        DataRow newDataRow = merge(dataRow, joinDataRow);
                        newDataGrid.getRows().add(newDataRow);
                    }
                }
            }
            return newDataGrid;
        }
    }

    public static DataRow merge(DataRow dataRow1, DataRow dataRow2){
        DataRow newDataRow = new DataRow();
        newDataRow.getCols().putAll(dataRow1.getCols());
        newDataRow.getCols().putAll(dataRow2.getCols());
        return newDataRow;
    }
}
