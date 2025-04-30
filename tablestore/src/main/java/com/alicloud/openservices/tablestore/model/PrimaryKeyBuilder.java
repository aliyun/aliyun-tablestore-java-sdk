package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class PrimaryKeyBuilder {
    List<PrimaryKeyColumn> primaryKeyColumns;

    private PrimaryKeyBuilder() {
        primaryKeyColumns = new ArrayList<PrimaryKeyColumn>();
    }

    private PrimaryKeyBuilder(int capacity) {
        primaryKeyColumns = new ArrayList<PrimaryKeyColumn>(capacity);
    }

    public static PrimaryKeyBuilder createPrimaryKeyBuilder() {
        return new PrimaryKeyBuilder();
    }

    public static PrimaryKeyBuilder createPrimaryKeyBuilder(int capacity) {
        return new PrimaryKeyBuilder();
    }

    public PrimaryKeyBuilder addPrimaryKeyColumn(PrimaryKeyColumn column) {
        Preconditions.checkNotNull(column, "The primary key column should not be null.");
        this.primaryKeyColumns.add(column);
        return this;
    }

    public PrimaryKeyBuilder addPrimaryKeyColumn(String name, PrimaryKeyValue value) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of primary key should not be null or empty.");
        Preconditions.checkNotNull(value, "The value of primary key should not be null.");

        this.primaryKeyColumns.add(new PrimaryKeyColumn(name, value));
        return this;
    }

    public PrimaryKey build() {
        return new PrimaryKey(primaryKeyColumns);
    }
}
