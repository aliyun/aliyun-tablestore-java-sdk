package com.alicloud.openservices.tablestore.model.search.agg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alicloud.openservices.tablestore.core.protocol.SearchAggregationBuilder;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Return the range of percentiles in the group.
 */
public class PercentilesAggregation implements Aggregation {

    private AggregationType aggregationType = AggregationType.AGG_PERCENTILES;

    private String aggName;
    /**
     * Field name
     */
    private String fieldName;
    /**
     * Percentile range
     * Required, for example: [0,90,99]
     */
    private List<Double> percentiles;
    /**
     * Default value for missing fields.
     * <p>If a document is missing this field, what default value should be used</p>
     */
    private ColumnValue missing;

    public PercentilesAggregation() {
    }

    private PercentilesAggregation(Builder builder) {
        setAggName(builder.aggName);
        setFieldName(builder.fieldName);
        setPercentiles(builder.percentiles);
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
        return SearchAggregationBuilder.buildPercentilesAggregation(this).toByteString();
    }

    public PercentilesAggregation setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }

    public String getFieldName() {
        return fieldName;
    }

    public PercentilesAggregation setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public List<Double> getPercentiles() {
        return percentiles;
    }

    public PercentilesAggregation setPercentiles(List<Double> percentiles) {
        this.percentiles = percentiles;
        return this;
    }

    public ColumnValue getMissing() {
        return missing;
    }

    public PercentilesAggregation setMissing(ColumnValue missing) {
        this.missing = missing;
        return this;
    }

    public static final class Builder implements AggregationBuilder {
        private String aggName;
        private String fieldName;
        private List<Double> percentiles;
        private ColumnValue missing;

        public Builder() {}

        public Builder aggName(String aggName) {
            this.aggName = aggName;
            return this;
        }

        public Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder percentiles(Double... percentile) {
            if (percentiles == null) {
                percentiles = new ArrayList<Double>();
            }
            this.percentiles.addAll(Arrays.asList(percentile));
            return this;
        }

        public Builder percentiles(List<Double> percentileList) {
            if (percentiles == null) {
                percentiles = new ArrayList<Double>();
            }
            this.percentiles.addAll(percentileList);
            return this;
        }

        public Builder missing(Object missing) {
            this.missing = ValueUtil.toColumnValue(missing);
            return this;
        }

        @Override
        public PercentilesAggregation build() {
            return new PercentilesAggregation(this);
        }
    }
}
