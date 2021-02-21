package com.fmer.tools.parallelsql.operation;

import com.fmer.tools.parallelsql.jdbc.TableData;
import lombok.Data;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.util.Set;

@Data
public abstract class BaseOperation implements Operation {
    /**
     * 此操作涉及的所有schema
     */
    private Set<String> schemaSet;
    public PlainSelect plainSelect = new PlainSelect();

    @Override
    public TableData fetchNext(Object param) {
        return null;
    }

    public void addSchema(String schema){
        schemaSet.add(schema);
    }
}
