package com.alicloud.openservices.tablestore.model.search.sort;

import com.alicloud.openservices.tablestore.model.search.ScanQuery;
import com.alicloud.openservices.tablestore.model.search.query.InnerHits;

/**
 * Sorts according to the storage order of data rows in the multi-field index.
 * <p> 1. The default sorting method used by {@link ScanQuery}; the order is mutable, so please do not rely on this sorting in your business logic.
 * <p> 2. The default sorting method for Nested child rows in {@link InnerHits}.
 * <p> 3. The default sorting method used during queries for indexes that include Nested fields.
 * <p> <b>This sorting method only supports explicit settings within InnerHits.</b>
 */
public class DocSort implements Sort.Sorter {
    private SortOrder order = SortOrder.ASC;

    public DocSort() {}

    public DocSort(SortOrder order) {
        this.order = order;
    }

    public SortOrder getOrder() {
        return order;
    }

    public void setOrder(SortOrder order) {
        this.order = order;
    }
}
