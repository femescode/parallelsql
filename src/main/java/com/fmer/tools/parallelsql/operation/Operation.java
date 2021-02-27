package com.fmer.tools.parallelsql.operation;

import com.fmer.tools.parallelsql.jdbc.TableData;
import com.fmer.tools.parallelsql.operation.bean.DataGrid;
import com.fmer.tools.parallelsql.operation.bean.DataRow;

import java.util.List;

public interface Operation {
    DataGrid fetchNext();
    void initParam(DataRow param);
}
