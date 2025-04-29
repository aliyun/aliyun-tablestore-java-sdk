package com.alicloud.openservices.tablestore.writer;


/**
 * Request management interface:
 * Implements two request construction methods: BatchWriteRowRequest and BulkImportRequest.
 */
public interface RequestManager {
    boolean appendRowChange(RowChangeWithGroup rowChangeWithGroup);

    RequestWithGroups makeRequest();

    void sendRequest(RequestWithGroups requestWithGroups);

    int getTotalRowsCount();

    void clear();
}
