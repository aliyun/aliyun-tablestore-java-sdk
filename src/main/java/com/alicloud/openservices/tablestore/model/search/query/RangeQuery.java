package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.google.protobuf.ByteString;

/**
 * 范围查询。通过设置一个范围（from，to），查询该范围内的所有数据。
 */
public class RangeQuery implements Query {

    /**
     * 字段名
     */
    private String fieldName;
    /**
     * 字段取值的下界
     */
    private ColumnValue from;
    /**
     * 字段取值的上界
     */
    private ColumnValue to;
    /**
     * 范围取值是否包含下届
     */
    private boolean includeLower;
    /**
     * 范围取值是否包含上届
     */
    private boolean includeUpper;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public void greaterThan(ColumnValue value) {
        this.setFrom(value, false);
    }

    public void greaterThanOrEqual(ColumnValue value) {
        this.setFrom(value, true);
    }

    public void lessThan(ColumnValue value) {
        this.setTo(value, false);
    }

    public void lessThanOrEqual(ColumnValue value) {
        this.setTo(value, true);
    }

    public void setFrom(ColumnValue value, boolean includeLower) {
        this.from = value;
        this.includeLower = includeLower;
    }

    public void setTo(ColumnValue value, boolean includeUpper) {
        this.to = value;
        this.includeUpper = includeUpper;
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.QueryType_RangeQuery;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildRangeQuery(this).toByteString();
    }

    public ColumnValue getFrom() {
        return from;
    }

    public void setFrom(ColumnValue from) {
        this.from = from;
    }

    public ColumnValue getTo() {
        return to;
    }

    public void setTo(ColumnValue to) {
        this.to = to;
    }

    public boolean isIncludeLower() {
        return includeLower;
    }

    public void setIncludeLower(boolean includeLower) {
        this.includeLower = includeLower;
    }

    public boolean isIncludeUpper() {
        return includeUpper;
    }

    public void setIncludeUpper(boolean includeUpper) {
        this.includeUpper = includeUpper;
    }
}
