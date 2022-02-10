package com.alicloud.openservices.tablestore.model.search.agg;

import com.alicloud.openservices.tablestore.core.protocol.SearchAggregationBuilder;
import com.google.protobuf.ByteString;

/**
 * 根据某一个字段统计文档数
 */
public class CountAggregation implements Aggregation {

    private AggregationType aggregationType = AggregationType.AGG_COUNT;

    /**
     * 聚合的名字，之后从聚合结果列表中根据该名字拿到聚合结果
     */
    private String aggName;
    /**
     * 字段名字
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
