package com.alicloud.openservices.tablestore.model.search.agg;

import com.alicloud.openservices.tablestore.core.protocol.SearchAggregationBuilder;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.google.protobuf.ByteString;

/**
 * 统计某一个字段的最小值
 * <p>举例：如果字段“age”恰好有5行（少一点我们方便举例），分别为：1、2、3、4、5，则进行 MinAggregation 的结果为1。</p>
 */
public class MinAggregation implements Aggregation {

    private AggregationType aggregationType = AggregationType.AGG_MIN;

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
         * 缺失字段的默认值。
         * <p>如果一个文档缺少该字段，则采用什么默认值</p>
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
