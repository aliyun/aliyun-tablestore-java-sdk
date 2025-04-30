package com.alicloud.openservices.tablestore.model.filter;

import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * TableStore supports setting a filter {@link Filter} during queries. Query operations include GetRow, BatchGetRow, GetRange. The filter processes the results on the server side before returning them.
 */
public interface Filter {

    FilterType getFilterType();

    ByteString serialize();
}
