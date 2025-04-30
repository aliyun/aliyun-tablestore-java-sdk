package com.alicloud.openservices.tablestore.model;

import java.util.List;
import java.util.Iterator;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class ListTableResponse extends Response implements Jsonizable {
    /**
     * List of table names.
     */
    private List<String> tableNames;
    
    public ListTableResponse(Response meta) {
        super(meta);
    }

    /**
     * Get the list of table names.
     * @return The list of table names.
     */
    public List<String> getTableNames() {
        return tableNames;
    }

    /*
     * Internal interface. Do not use.
     */
    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        if (tableNames == null) {
            sb.append("null");
        } else {
            sb.append("[");
            Iterator<String> iter = tableNames.listIterator();
            if (iter.hasNext()) {
                sb.append(iter.next());
                for(; iter.hasNext();) {
                    sb.append(", \"");
                    sb.append(iter.next());
                    sb.append("\"");
                }
            }
            sb.append("]");
        }
    }
}
