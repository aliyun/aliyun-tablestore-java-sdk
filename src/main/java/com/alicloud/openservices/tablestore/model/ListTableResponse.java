package com.alicloud.openservices.tablestore.model;

import java.util.List;
import java.util.Iterator;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class ListTableResponse extends Response implements Jsonizable {
    /**
     * 表的名称列表。
     */
    private List<String> tableNames;
    
    public ListTableResponse(Response meta) {
        super(meta);
    }

    /**
     * 获取表的名称列表。
     * @return 表的名称列表。
     */
    public List<String> getTableNames() {
        return tableNames;
    }

    /*
     * 内部接口。请勿使用。
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
