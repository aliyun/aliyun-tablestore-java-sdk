package com.alicloud.openservices.tablestore.writer;


/**
 * 请求管理接口：
 * 实现BatchWriteRowRequest、BulkImportRequest两种请求构建方式
 */
public interface RequestManager {
    boolean appendRowChange(RowChangeWithGroup rowChangeWithGroup);

    RequestWithGroups makeRequest();

    void sendRequest(RequestWithGroups requestWithGroups);

    int getTotalRowsCount();

    void clear();
}
