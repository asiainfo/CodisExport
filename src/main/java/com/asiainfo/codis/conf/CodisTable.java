package com.asiainfo.codis.conf;

import java.util.Map;

/**
 * Created by peng on 16/9/7.
 */
public class CodisTable {
    private String header;
    private Map<String, Condition> conditions;
    private String where;

    public CodisTable(String header, Map<String, Condition> conditions) {
        this.header = header;
        this.conditions = conditions;
    }




    @Override
    public String toString() {
        return "CodisTable{" +
                "header='" + header + '\'' +
                ", conditions=" + conditions +
                '}';
    }

    public String getHeader() {
        return header;
    }

    public Map<String, Condition> getConditions() {
        return conditions;
    }

    public String getWhere() {
        return where;
    }
}
