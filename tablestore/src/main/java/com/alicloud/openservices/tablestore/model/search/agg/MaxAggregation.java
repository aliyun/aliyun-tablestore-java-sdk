package com.alicloud.openservices.tablestore.model.search.agg;

import com.alicloud.openservices.tablestore.core.protocol.SearchAggregationBuilder;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Calculate the maximum value of a specific field.
 * <p>Example: If the field "age" has exactly 5 rows (fewer for simplicity), with values 1, 2, 3, 4, 5, then the result of MaxAggregation would be 5.</p>
 */
public class MaxAggregation implements Aggregation {

    private AggregationType aggregationType = AggregationType.AGG_MAX;

    /**
     * The name of the aggregation, which is used to retrieve the aggregation result from the aggregation result list later.
     */
    private String aggName;
    /**
     * Field name
     */
    private String fieldName;
    /**
     * Default value for missing fields.
     * <p>If a document is missing this field, what default value should be used</p>
     */
    private ColumnValue missing;

    public MaxAggregation() {
    }

    private MaxAggregation(Builder builder) {
        aggName = builder.aggName;
        fieldName = builder.fieldName;
        missing = builder.missing;
    }
    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public String getAggName() {
        return aggName;
    }

    @Override
    public AggregationType getAggType() {
        return aggregationType;
    }

    @Override
    public ByteString serialize() {
        return SearchAggregationBuilder.buildMaxAggregation(this).toByteString();
    }

    public String getFieldName() {
        return fieldName;
    }

    public ColumnValue getMissing() {
        return missing;
    }

    public MaxAggregation setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }

    public MaxAggregation setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public MaxAggregation setMissing(ColumnValue missing) {
        this.missing = missing;
        return this;
    }

    public static final class Builder implements AggregationBuilder {
        private String aggName;
        private String fieldName;
        private ColumnValue missing;

        private Builder() {}

        public Builder aggName(String aggName) {
            this.aggName = aggName;
            return this;
        }

        public Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }
        /**
         * Default value for missing fields.
         * <p>If a document is missing this field, what default value should be used</p>
         */
        public Builder missing(Object missing) {
            this.missing = ValueUtil.toColumnValue(missing);
            return this;
        }

        @Override
        public MaxAggregation build() {
            return new MaxAggregation(this);
        }
    }
}
