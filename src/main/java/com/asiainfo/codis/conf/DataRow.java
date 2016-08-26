package com.asiainfo.codis.conf;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class DataRow {
    private String line;

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public String getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(String primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    private String primaryKeys;


    public DataRow(String line, String primaryKeys) {
        this.line = line;
        this.primaryKeys = primaryKeys;
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder().append(getPrimaryKeys()).toHashCode();

    }
    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
        if (o == this)
            return true;
        if (o.getClass() != getClass())
            return false;

        DataRow row = (DataRow) o;
        return new EqualsBuilder().append(getPrimaryKeys(), row.getPrimaryKeys()).isEquals();
    }
}
