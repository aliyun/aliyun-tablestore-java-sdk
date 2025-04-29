package com.alicloud.openservices.tablestore.model.search.sort;

/**
 * Select which value of a Field to use for sorting.
 * <p>In most cases, a field has only one value, so this option is meaningless and defaults to MIN.</p>
 * <p>When a field is a collection (Array), you can specify one of the values in the collection to be used as the basis for sorting.</p>
 */
public enum SortMode {
    MIN,
    MAX,
    AVG
}
