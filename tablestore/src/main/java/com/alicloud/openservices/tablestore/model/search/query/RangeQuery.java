package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Range query. By setting a range (from, to), query all the data within this range.
 */
public class RangeQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_RangeQuery;
    /**
     * Field name
     */
    private String fieldName;
    /**
     * The lower bound of the field value
     */
    private ColumnValue from;
    /**
     * The upper bound of the field value
     */
    private ColumnValue to;
    /**
     * Whether the range value includes the lower bound
     */
    private boolean includeLower;
    /**
     * Whether the range value includes the upper bound
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
        return queryType;
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

    protected static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder implements QueryBuilder {
        private String fieldName;
        private ColumnValue from;
        private ColumnValue to;
        private boolean includeLower;
        private boolean includeUpper;

        private Builder() {}

        public Builder field(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder greaterThanOrEqual(Object value) {
            this.from = ValueUtil.toColumnValue(value);
            this.includeLower = true;
            return this;
        }

        public Builder greaterThan(Object value) {
            this.from = ValueUtil.toColumnValue(value);
            this.includeLower = false;
            return this;
        }

        public Builder lessThanOrEqual(Object value) {
            this.to = ValueUtil.toColumnValue(value);
            this.includeUpper = true;
            return this;
        }

        public Builder lessThan(Object value) {
            this.to = ValueUtil.toColumnValue(value);
            this.includeUpper = false;
            return this;
        }

        @Override
        public RangeQuery build() {
            RangeQuery rangeQuery = new RangeQuery();
            rangeQuery.setFieldName(this.fieldName);
            rangeQuery.setFrom(this.from);
            rangeQuery.setTo(this.to);
            rangeQuery.setIncludeLower(this.includeLower);
            rangeQuery.setIncludeUpper(this.includeUpper);
            return rangeQuery;
        }

    }
}
