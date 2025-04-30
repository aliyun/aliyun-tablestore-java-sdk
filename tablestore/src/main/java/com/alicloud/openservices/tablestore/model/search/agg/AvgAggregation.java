package com.alicloud.openservices.tablestore.model.search.agg;

import com.alicloud.openservices.tablestore.core.protocol.SearchAggregationBuilder;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Calculate the average value of a field.
 * <p>Example: If the field "age" has exactly 5 rows (fewer for convenient illustration), with values 1, 2, 3, 4, and 5, respectively, then the result of AvgAggregation would be 3.</p>
 */
public class AvgAggregation implements Aggregation {

    private AggregationType aggregationType = AggregationType.AGG_AVG;

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

    public AvgAggregation() {
    }

    private AvgAggregation(Builder builder) {
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
        return SearchAggregationBuilder.buildAvgAggregation(this).toByteString();
    }

    public String getFieldName() {
        return fieldName;
    }

    public ColumnValue getMissing() {
        return missing;
    }

    public AvgAggregation setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }

    public AvgAggregation setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public AvgAggregation setMissing(ColumnValue missing) {
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
        public AvgAggregation build() {
            return new AvgAggregation(this);
        }
    }
}
