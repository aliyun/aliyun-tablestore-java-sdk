package com.alicloud.openservices.tablestore.model.search.agg;

import com.alicloud.openservices.tablestore.core.protocol.SearchAggregationBuilder;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Calculate the minimum value of a specific field.
 * <p>Example: If the "age" field has exactly 5 rows (fewer for simplicity), with values: 1, 2, 3, 4, 5, then the result of MinAggregation would be 1.</p>
 */
public class MinAggregation implements Aggregation {

    private AggregationType aggregationType = AggregationType.AGG_MIN;

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

    public MinAggregation() {
    }

    private MinAggregation(Builder builder) {
        setAggName(builder.aggName);
        setFieldName(builder.fieldName);
        setMissing(builder.missing);
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
        return SearchAggregationBuilder.buildMinAggregation(this).toByteString();
    }

    public void setAggName(String aggName) {
        this.aggName = aggName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public ColumnValue getMissing() {
        return missing;
    }

    public void setMissing(ColumnValue missing) {
        this.missing = missing;
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
        public MinAggregation build() {
            return new MinAggregation(this);
        }
    }
}
