package com.alicloud.openservices.tablestore.model.search.agg;

import com.alicloud.openservices.tablestore.core.protocol.SearchAggregationBuilder;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.google.protobuf.ByteString;

/**
 * 求某一个字段的和
 * <p>举例：如果字段“age”恰好有5行（少一点我们方便举例），分别为：1、2、3、4、5，则进行 SumAggregation 的结果为15。</p>
 */
public class SumAggregation implements Aggregation {

    private AggregationType aggregationType = AggregationType.AGG_SUM;

    /**
     * 聚合的名字，之后从聚合结果列表中根据该名字拿到聚合结果
     */
    private String aggName;
    /**
     * 字段名字
     */
    private String fieldName;
    /**
     * 缺失字段的默认值。
     * <p>如果一个文档缺少该字段，则采用什么默认值</p>
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
         * 缺失字段的默认值。
         * <p>如果一个文档缺少该字段，则采用什么默认值</p>
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
