package com.fmer.tools.parallelsql.operation;

public class JoinOperation extends BaseOperation {
    private BaseOperation from;
    private BaseOperation join;

    public JoinOperation(BaseOperation from, BaseOperation join) {
        this.from = from;
        this.join = join;
    }
}
