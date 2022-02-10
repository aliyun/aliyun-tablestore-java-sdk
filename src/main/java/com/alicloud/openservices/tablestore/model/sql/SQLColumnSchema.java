package com.alicloud.openservices.tablestore.model.sql;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.ColumnType;

/**
 * 表示 SQL 表的列结构信息
 */
public class SQLColumnSchema implements Jsonizable {

    private String name;
    private ColumnType type;

    public SQLColumnSchema(String name, ColumnType type) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "SQL col's name should not be null or empty.");
        Preconditions.checkNotNull(type, "The type should not be null.");

        this.name = name;
        this.type = type;
    }

    /**
     * 获取 SQL 列的名称
     * @return SQL 列的名称
     */
    public String getName() {
        return name;
    }

    /**
     * 获取 SQL 列的类型
     * @return SQL 列的类型
     */
    public ColumnType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof SQLColumnSchema)) {
            return false;
        }

        SQLColumnSchema target = (SQLColumnSchema) o;
        return this.name.equals(target.name) && this.type == target.type;
    }

    @Override
    public int hashCode() {
        return this.name.hashCode() ^ this.type.hashCode();
    }

    @Override
    public String toString() {
        return name + ":" + type;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append(String.format(
                "{\"Name\": \"%s\", \"Type\": \"%s\"}",
                name, type.toString()));

    }
}
