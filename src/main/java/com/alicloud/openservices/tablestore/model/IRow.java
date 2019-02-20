package com.alicloud.openservices.tablestore.model;

public interface IRow extends Comparable<IRow> {
    public PrimaryKey getPrimaryKey();
}
