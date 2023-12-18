package com.alicloud.openservices.tablestore.model.search.sort;

import com.alicloud.openservices.tablestore.model.search.ScanQuery;
import com.alicloud.openservices.tablestore.model.search.query.InnerHits;

/**
 * 按照数据行在多元索引中的存储顺序排序
 * <p> 1. {@link ScanQuery} 默认采取的排序方式;顺序具有易变性,业务上请勿依赖此排序
 * <p> 2. {@link InnerHits} Nested子行的默认排序方式
 * <p> 3. 包含Nested字段的索引，查询时默认采取的排序方式
 * <p> <b>该排序方式仅支持在InnerHits内显式设置</b>
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
