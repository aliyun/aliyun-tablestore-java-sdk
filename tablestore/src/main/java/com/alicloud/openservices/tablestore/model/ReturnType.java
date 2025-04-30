package com.alicloud.openservices.tablestore.model;

/**
 * Indicates whether the operation (PUT, UPDATE, DELETE) returns the PK value in the result. For PK auto-increment columns, returning the PK should be set.
 */
public enum ReturnType {
    /**
     * Do not return any row data.
     */
	RT_NONE,

    /**
     * Returns the data of the PK column.
     */
	RT_PK,

    /**
     * Returns the data of the modified column (e.g., returns the result of atomic addition).
     */
    RT_AFTER_MODIFY,
}
