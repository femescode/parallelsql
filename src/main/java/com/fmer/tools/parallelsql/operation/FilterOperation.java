package com.fmer.tools.parallelsql.operation;

public class FilterOperation extends BaseOperation {
    private BaseOperation query;

    public FilterOperation(BaseOperation query) {
        this.query = query;
    }
}
