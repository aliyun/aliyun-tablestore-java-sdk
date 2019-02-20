package com.alicloud.openservices.tablestore.model.filter;

import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.google.protobuf.ByteString;

public class ColumnPaginationFilter implements Filter {

    private int limit;
    private int offset;

    public ColumnPaginationFilter(int limit) {
        this.limit = limit;
        this.offset = 0;
    }

    public ColumnPaginationFilter(int limit, int offset) {
        this.limit = limit;
        this.offset = offset;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.COLUMN_PAGINATION_FILTER;
    }

    @Override
    public ByteString serialize() {
        return OTSProtocolBuilder.buildColumnPaginationFilter(this);
    }
}
