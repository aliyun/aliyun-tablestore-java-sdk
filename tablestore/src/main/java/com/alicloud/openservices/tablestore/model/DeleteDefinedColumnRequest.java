package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * DeleteDefinedColumn includes some parameters required to delete a predefined column for a table, including the table name and the name of the predefined column to be deleted.
 */
public class DeleteDefinedColumnRequest implements Request {
    private String tableName;
    private List<String> definedColumns = new ArrayList<String>();

    /**
     * Sets the name of the table.
     *
     * @param tableName The name of the table.
     */
    public void setTableName(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The name of table should not be null or empty.");

        this.tableName = tableName;
    }

    /**
     * Get the table name
     * @return the name of the table
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Add a predefined column
     * @param name The name of the predefined column.
     */
    public void addDefinedColumn(String name) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of defined column should not be null or empty.");

        this.definedColumns.add(name);
    }

    /**
     * Get the predefined column list
     * @return Predefined column list
     */
    public List<String> getDefinedColumn() {
        return Collections.unmodifiableList(definedColumns);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_DEFINED_COLUMN;
    }
}

