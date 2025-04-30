package com.alicloud.openservices.tablestore.model.search.agg;

import com.alicloud.openservices.tablestore.core.protocol.SearchAggregationBuilder;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Count the total number of documents after deduplication based on a specific field. 
 * This total is an approximate value, and there may be some errors when the data volume is extremely large.
 */
public class DistinctCountAggregation implements Aggregation {

    private AggregationType aggregationType = AggregationType.AGG_DISTINCT_COUNT;

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

    public DistinctCountAggregation() {
    }

    private DistinctCountAggregation(Builder builder) {
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
        return SearchAggregationBuilder.buildDistinctCountAggregation(this).toByteString();
    }

    public String getFieldName() {
        return fieldName;
    }

    public ColumnValue getMissing() {
        return missing;
    }

    public DistinctCountAggregation setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }

    public DistinctCountAggregation setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public DistinctCountAggregation setMissing(ColumnValue missing) {
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
        public DistinctCountAggregation build() {
            return new DistinctCountAggregation(this);
        }
    }
}
