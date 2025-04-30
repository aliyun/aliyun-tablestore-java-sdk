package com.alicloud.openservices.tablestore.model.sql;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.ColumnType;

/**
 * Represents the column structure information of an SQL table
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
     * Get the name of the SQL column
     * @return the name of the SQL column
     */
    public String getName() {
        return name;
    }

    /**
     * Get the type of SQL column
     * @return the type of SQL column
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
