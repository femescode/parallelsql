package com.fmer.tools.parallelsql.operation;

import com.fmer.tools.parallelsql.operation.bean.DataGrid;

public interface DataFetcher {
    DataGrid fetchNext();
}
