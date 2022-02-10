package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.protocol.Search.AggregationType;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AvgAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.CountAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.DistinctCountAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.MaxAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.MinAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.PercentilesAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.SumAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.TopRowsAggregation;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link Aggregation} deserialization tool class. For serialization, please refer to {@link SearchAggregationBuilder}
 */
public class SearchAggregationParser {

    private static MaxAggregation toMaxAggregation(String aggName, ByteString aggByteString) throws IOException {
        Search.MaxAggregation pb = Search.MaxAggregation.parseFrom(aggByteString);
        MaxAggregation agg = new MaxAggregation();
        agg.setAggName(aggName);
        if (pb.hasFieldName()) {
            agg.setFieldName(pb.getFieldName());
        }
        if (pb.hasMissing()) {
            ColumnValue columnValue = ValueUtil.toColumnValue(SearchVariantType.getValue(pb.getMissing().toByteArray()));
            agg.setMissing(columnValue);
        }
        return agg;
    }

    private static AvgAggregation toAvgAggregation(String aggName, ByteString aggByteString) throws IOException {
        Search.AvgAggregation pb = Search.AvgAggregation.parseFrom(aggByteString);
        AvgAggregation agg = new AvgAggregation();
        agg.setAggName(aggName);
        if (pb.hasFieldName()) {
            agg.setFieldName(pb.getFieldName());
        }
        if (pb.hasMissing()) {
            ColumnValue columnValue = ValueUtil.toColumnValue(SearchVariantType.getValue(pb.getMissing().toByteArray()));
            agg.setMissing(columnValue);
        }
        return agg;
    }

    private static MinAggregation toMinAggregation(String aggName, ByteString aggByteString) throws IOException {
        Search.MinAggregation pb = Search.MinAggregation.parseFrom(aggByteString);
        MinAggregation agg = new MinAggregation();
        agg.setAggName(aggName);
        if (pb.hasFieldName()) {
            agg.setFieldName(pb.getFieldName());
        }
        if (pb.hasMissing()) {
            ColumnValue columnValue = ValueUtil.toColumnValue(SearchVariantType.getValue(pb.getMissing().toByteArray()));
            agg.setMissing(columnValue);
        }
        return agg;
    }

    private static SumAggregation toSumAggregation(String aggName, ByteString aggByteString) throws IOException {
        Search.SumAggregation pb = Search.SumAggregation.parseFrom(aggByteString);
        SumAggregation agg = new SumAggregation();
        agg.setAggName(aggName);
        if (pb.hasFieldName()) {
            agg.setFieldName(pb.getFieldName());
        }
        if (pb.hasMissing()) {
            ColumnValue columnValue = ValueUtil.toColumnValue(SearchVariantType.getValue(pb.getMissing().toByteArray()));
            agg.setMissing(columnValue);
        }
        return agg;
    }

    private static CountAggregation toCountAggregation(String aggName, ByteString aggByteString) throws IOException {
        Search.CountAggregation pb = Search.CountAggregation.parseFrom(aggByteString);
        CountAggregation agg = new CountAggregation();
        agg.setAggName(aggName);
        if (pb.hasFieldName()) {
            agg.setFieldName(pb.getFieldName());
        }
        return agg;
    }

    private static DistinctCountAggregation toDistinctCountAggregation(String aggName, ByteString aggByteString) throws IOException {
        Search.DistinctCountAggregation pb = Search.DistinctCountAggregation.parseFrom(aggByteString);
        DistinctCountAggregation agg = new DistinctCountAggregation();
        agg.setAggName(aggName);
        if (pb.hasFieldName()) {
            agg.setFieldName(pb.getFieldName());
        }
        if (pb.hasMissing()) {
            ColumnValue columnValue = ValueUtil.toColumnValue(SearchVariantType.getValue(pb.getMissing().toByteArray()));
            agg.setMissing(columnValue);
        }
        return agg;
    }

    private static TopRowsAggregation toTopRowsAggregation(String aggName, ByteString aggByteString) throws IOException {
        Search.TopRowsAggregation pb = Search.TopRowsAggregation.parseFrom(aggByteString);
        TopRowsAggregation agg = new TopRowsAggregation();
        agg.setAggName(aggName);
        if (pb.hasLimit()) {
            agg.setLimit(pb.getLimit());
        }
        if (pb.hasSort()) {
            agg.setSort(SearchSortParser.toSort(pb.getSort()));
        }
        return agg;
    }

    private static PercentilesAggregation toPercentilesAggregation(String aggName, ByteString aggByteString) throws IOException {
        Search.PercentilesAggregation pb = Search.PercentilesAggregation.parseFrom(aggByteString);
        PercentilesAggregation agg = new PercentilesAggregation();
        agg.setAggName(aggName);
        if (pb.hasFieldName()) {
            agg.setFieldName(pb.getFieldName());
        }
        if (pb.hasMissing()) {
            ColumnValue columnValue = ValueUtil.toColumnValue(SearchVariantType.getValue(pb.getMissing().toByteArray()));
            agg.setMissing(columnValue);
        }
        agg.setPercentiles(pb.getPercentilesList());
        return agg;
    }

    public static Aggregation toAggregation(Search.Aggregation pb) throws IOException {
        ByteString body = pb.getBody();
        String aggName = pb.getName();
        AggregationType type = pb.getType();
        switch (type) {
            case AGG_AVG:
                return toAvgAggregation(aggName, body);
            case AGG_MIN:
                return toMinAggregation(aggName, body);
            case AGG_MAX:
                return toMaxAggregation(aggName, body);
            case AGG_SUM:
                return toSumAggregation(aggName, body);
            case AGG_COUNT:
                return toCountAggregation(aggName, body);
            case AGG_DISTINCT_COUNT:
                return toDistinctCountAggregation(aggName, body);
            case AGG_TOP_ROWS:
                return toTopRowsAggregation(aggName, body);
            case AGG_PERCENTILES:
                return toPercentilesAggregation(aggName, body);
            default:
                throw new IllegalArgumentException("unknown AggregationType: " + type.name());
        }
    }

    public static List<Aggregation> toAggregations(Search.Aggregations pb) throws IOException {
        List<Aggregation> aggregations = new ArrayList<Aggregation>();
        for (Search.Aggregation aggregation : pb.getAggsList()) {
            aggregations.add(toAggregation(aggregation));
        }
        return aggregations;
    }
}
