package com.alicloud.openservices.tablestore.model.filter;

import com.google.protobuf.ByteString;

/**
 * TableStore支持在查询时设置过滤器{@link Filter}, 查询操作包括GetRow, BatchGetRow, GetRange, 过滤器会在服务端对查询出的结果进行过滤后返回.
 */
public interface Filter {

    FilterType getFilterType();

    ByteString serialize();
}
