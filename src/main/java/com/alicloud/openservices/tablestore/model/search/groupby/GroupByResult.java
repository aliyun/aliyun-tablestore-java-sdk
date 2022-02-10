package com.alicloud.openservices.tablestore.model.search.groupby;

/**
 * GroupBy 结果的接口，具体说明请看具体实现里的说明
 */
public interface GroupByResult {

    String getGroupByName();

    GroupByType getGroupByType();

}
