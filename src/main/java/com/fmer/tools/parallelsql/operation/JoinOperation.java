package com.fmer.tools.parallelsql.operation;

import com.fmer.tools.parallelsql.operation.bean.DataGrid;
import com.fmer.tools.parallelsql.operation.bean.DataRow;

public class JoinOperation extends BaseOperation {
    private boolean isLeftJoin;
    private BaseOperation from;
    private BaseOperation join;

    public JoinOperation(BaseOperation from, BaseOperation join) {
        this.from = from;
        this.join = join;
    }

    @Override
    public DataGrid fetchNext() {
        DataGrid tableData = from.fetchNext();
        if(tableData == null){
            return null;
        }
        DataGrid newTableData = new DataGrid();
        newTableData.getFieldInfos().addAll(tableData.getFieldInfos());
        for (DataRow dataRow : tableData.getRows()) {
            join.initParam(dataRow);
            DataGrid joinData = join.fetchNext();
            if (joinData != null) {
                newTableData.getFieldInfos().addAll(joinData.getFieldInfos());
                for(DataRow joinRowData : joinData.getRows()){
                    DataRow newDataRow = merge(dataRow, joinRowData);
                    newTableData.getRows().add(newDataRow);
                }
            }
        }
        return newTableData;
    }

    public static DataRow merge(DataRow dataRow1, DataRow dataRow2){
        DataRow newDataRow = new DataRow();
        newDataRow.getCols().putAll(dataRow1.getCols());
        newDataRow.getCols().putAll(dataRow2.getCols());
        return newDataRow;
    }
}
