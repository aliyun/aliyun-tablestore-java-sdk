package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * AddDefinedColumn includes some necessary parameters for adding a predefined column to a table, including the table name, the name and type of the predefined column to be added.
 */
public class AddDefinedColumnRequest implements Request {
    private String tableName;
    private List<DefinedColumnSchema> definedColumns = new ArrayList<DefinedColumnSchema>();

    /**
     * Set the name of the table.
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
     * Add a predefined column.
     * @param name The name of the predefined column.
     * @param type The data type of the predefined column.
     */
    public void addDefinedColumn(String name, DefinedColumnType type) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of defined column should not be null or empty.");
        Preconditions.checkNotNull(type, "The type of defined column should not be null.");

        this.definedColumns.add(new DefinedColumnSchema(name, type));
    }

    /**
     * Get the predefined column list
     * @return Predefined column list
     */
    public List<DefinedColumnSchema> getDefinedColumn() {
        return Collections.unmodifiableList(definedColumns);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_ADD_DEFINED_COLUMN;
    }
}

