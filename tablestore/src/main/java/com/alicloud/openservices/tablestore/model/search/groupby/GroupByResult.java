package com.alicloud.openservices.tablestore.model.search.groupby;

/**
 * Interface for GroupBy results. For detailed explanations, please refer to the specific implementations.
 */
public interface GroupByResult {

    String getGroupByName();

    GroupByType getGroupByType();

}
