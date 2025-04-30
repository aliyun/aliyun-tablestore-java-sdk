package com.alicloud.openservices.tablestore.model.search.agg;

import com.alicloud.openservices.tablestore.core.protocol.SearchAggregationBuilder;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Calculate the sum of a specific field.
 * <p>Example: If the "age" field has exactly 5 rows (fewer for simplicity), with values: 1, 2, 3, 4, 5, then the result of SumAggregation would be 15.</p>
 */
public class SumAggregation implements Aggregation {

    private AggregationType aggregationType = AggregationType.AGG_SUM;

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
     * <p>If a document is missing this field, what default value should be used.</p>
     */
    private ColumnValue missing;

    public SumAggregation() {
    }

    private SumAggregation(Builder builder) {
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
        return SearchAggregationBuilder.buildSumAggregation(this).toByteString();
    }

    public String getFieldName() {
        return fieldName;
    }

    public ColumnValue getMissing() {
        return missing;
    }

    public SumAggregation setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }

    public SumAggregation setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public SumAggregation setMissing(ColumnValue missing) {
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
        public SumAggregation build() {
            return new SumAggregation(this);
        }
    }
}
