package com.fmer.tools.parallelsql.operation;

import com.fmer.tools.parallelsql.jdbc.TableData;

import java.util.List;

public interface Operation {
    TableData fetchNext(Object param);
}
