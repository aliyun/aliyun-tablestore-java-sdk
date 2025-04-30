package com.alicloud.openservices.tablestore.model.search.agg;

import com.alicloud.openservices.tablestore.core.protocol.SearchAggregationBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Counts the number of documents based on a specific field.
 */
public class CountAggregation implements Aggregation {

    private AggregationType aggregationType = AggregationType.AGG_COUNT;

    /**
     * The name of the aggregation, which is used to retrieve the aggregation result from the aggregation result list later.
     */
    private String aggName;
    /**
     * Field name
     */
    private String fieldName;

    private CountAggregation(Builder builder) {
        aggName = builder.aggName;
        fieldName = builder.fieldName;
    }

    public CountAggregation() {
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
        return SearchAggregationBuilder.buildCountAggregation(this).toByteString();
    }

    public String getFieldName() {
        return fieldName;
    }


    public CountAggregation setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }

    public CountAggregation setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public static final class Builder implements AggregationBuilder {
        private String aggName;
        private String fieldName;

        private Builder() {}

        public Builder aggName(String aggName) {
            this.aggName = aggName;
            return this;
        }

        public Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        @Override
        public CountAggregation build() {
            return new CountAggregation(this);
        }
    }
}
