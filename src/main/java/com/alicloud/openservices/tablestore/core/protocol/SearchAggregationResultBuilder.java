package com.alicloud.openservices.tablestore.core.protocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationResult;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationResults;
import com.alicloud.openservices.tablestore.model.search.agg.AvgAggregationResult;
import com.alicloud.openservices.tablestore.model.search.agg.CountAggregationResult;
import com.alicloud.openservices.tablestore.model.search.agg.DistinctCountAggregationResult;
import com.alicloud.openservices.tablestore.model.search.agg.MaxAggregationResult;
import com.alicloud.openservices.tablestore.model.search.agg.MinAggregationResult;
import com.alicloud.openservices.tablestore.model.search.agg.PercentilesAggregationItem;
import com.alicloud.openservices.tablestore.model.search.agg.PercentilesAggregationResult;
import com.alicloud.openservices.tablestore.model.search.agg.SumAggregationResult;
import com.alicloud.openservices.tablestore.model.search.agg.TopRowsAggregationResult;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

class SearchAggregationResultBuilder {

    private static AvgAggregationResult buildAvgAggregationResult(String aggName, ByteString aggBody)
        throws InvalidProtocolBufferException {
        Search.AvgAggregationResult aggResult = Search.AvgAggregationResult.parseFrom(aggBody);
        AvgAggregationResult result = new AvgAggregationResult();
        result.setAggName(aggName);
        result.setValue(aggResult.getValue());
        return result;
    }

    private static MaxAggregationResult buildMaxAggregationResult(String aggName, ByteString aggBody)
        throws InvalidProtocolBufferException {
        Search.MaxAggregationResult aggResult = Search.MaxAggregationResult.parseFrom(aggBody);
        MaxAggregationResult result = new MaxAggregationResult();
        result.setAggName(aggName);
        result.setValue(aggResult.getValue());
        return result;
    }

    private static MinAggregationResult buildMinAggregationResult(String aggName, ByteString aggBody)
        throws InvalidProtocolBufferException {
        Search.MinAggregationResult aggResult = Search.MinAggregationResult.parseFrom(aggBody);
        MinAggregationResult result = new MinAggregationResult();
        result.setAggName(aggName);
        result.setValue(aggResult.getValue());
        return result;
    }

    private static SumAggregationResult buildSumAggregationResult(String aggName, ByteString aggBody)
        throws InvalidProtocolBufferException {
        Search.SumAggregationResult aggResult = Search.SumAggregationResult.parseFrom(aggBody);
        SumAggregationResult result = new SumAggregationResult();
        result.setAggName(aggName);
        result.setValue(aggResult.getValue());
        return result;
    }

    private static DistinctCountAggregationResult buildDistinctCountAggregationResult(String aggName,
        ByteString aggBody)
        throws InvalidProtocolBufferException {
        Search.DistinctCountAggregationResult aggResult = Search.DistinctCountAggregationResult.parseFrom(aggBody);
        DistinctCountAggregationResult result = new DistinctCountAggregationResult();
        result.setAggName(aggName);
        result.setValue(aggResult.getValue());
        return result;
    }

    private static CountAggregationResult buildCountAggregationResult(String aggName, ByteString aggBody)
        throws InvalidProtocolBufferException {
        Search.CountAggregationResult aggResult = Search.CountAggregationResult.parseFrom(aggBody);
        CountAggregationResult result = new CountAggregationResult();
        result.setAggName(aggName);
        result.setValue(aggResult.getValue());
        return result;
    }

    private static TopRowsAggregationResult buildTopRowsAggregationResult(String aggName, ByteString aggBody)
        throws IOException {
        Search.TopRowsAggregationResult aggResult = Search.TopRowsAggregationResult.parseFrom(aggBody);
        TopRowsAggregationResult result = new TopRowsAggregationResult();
        result.setAggName(aggName);
        // build rows by plainBuffer
        List<Row> rows = new ArrayList<Row>(aggResult.getRowsCount());
        for (ByteString byteString : aggResult.getRowsList()) {
            rows.add(parseRow(byteString));
        }
        result.setRows(rows);
        return result;
    }

    private static PercentilesAggregationResult buildPercentilesAggregationResult(String aggName, ByteString aggBody)
        throws IOException {
        Search.PercentilesAggregationResult aggResult = Search.PercentilesAggregationResult.parseFrom(aggBody);
        PercentilesAggregationResult result = new PercentilesAggregationResult();
        result.setAggName(aggName);
        // build percentilesAggregationItem by plainBuffer
        List<PercentilesAggregationItem> percentilesAggregationItems = new ArrayList<PercentilesAggregationItem>(aggResult.getPercentilesAggregationItemsCount());
        for (Search.PercentilesAggregationItem item : aggResult.getPercentilesAggregationItemsList()) {
            percentilesAggregationItems.add(buildPercentilesAggregationItem(item));
        }
        result.setPercentilesAggregationItems(percentilesAggregationItems);
        return result;
    }

    public static Row parseRow(ByteString byteString) throws IOException {
        PlainBufferCodedInputStream coded = new PlainBufferCodedInputStream(new PlainBufferInputStream(byteString.asReadOnlyByteBuffer()));
        List<PlainBufferRow> plainBufferRows = coded.readRowsWithHeader();
        if (plainBufferRows.size() != 1) {
            throw new IOException("Expect only returns one row. Row count: " + plainBufferRows.size());
        }
        return PlainBufferConversion.toRow(plainBufferRows.get(0));
    }

    private static AggregationResult buildAggResult(Search.AggregationResult aggregationResult)
        throws IOException {
        switch (aggregationResult.getType()) {
            case AGG_AVG:
                return buildAvgAggregationResult(aggregationResult.getName(), aggregationResult.getAggResult());
            case AGG_MAX:
                return buildMaxAggregationResult(aggregationResult.getName(), aggregationResult.getAggResult());
            case AGG_MIN:
                return buildMinAggregationResult(aggregationResult.getName(), aggregationResult.getAggResult());
            case AGG_SUM:
                return buildSumAggregationResult(aggregationResult.getName(), aggregationResult.getAggResult());
            case AGG_DISTINCT_COUNT:
                return buildDistinctCountAggregationResult(aggregationResult.getName(),
                    aggregationResult.getAggResult());
            case AGG_COUNT:
                return buildCountAggregationResult(aggregationResult.getName(), aggregationResult.getAggResult());
            case AGG_TOP_ROWS:
                return buildTopRowsAggregationResult(aggregationResult.getName(), aggregationResult.getAggResult());
            case AGG_PERCENTILES:
                return buildPercentilesAggregationResult(aggregationResult.getName(), aggregationResult.getAggResult());
            default:
                throw new ClientException("unsupported aggType: " + aggregationResult.getType());
        }
    }

    static AggregationResults buildAggregationResults(Search.AggregationsResult aggregationsResult)
        throws IOException {
        AggregationResults aggregationResults = new AggregationResults();
        Map<String, AggregationResult> map = new HashMap<String, AggregationResult>();

        for (Search.AggregationResult s : aggregationsResult.getAggResultsList()) {
            map.put(s.getName(), buildAggResult(s));
        }
        aggregationResults.setResultMap(map);
        return aggregationResults;
    }

    static AggregationResults buildAggregationResultsFromByteString(ByteString agg)
        throws IOException {
        Search.AggregationsResult aggregationsResult = Search.AggregationsResult.parseFrom(agg);
        return buildAggregationResults(aggregationsResult);
    }

    private static PercentilesAggregationItem buildPercentilesAggregationItem(
        Search.PercentilesAggregationItem percentilesAggregationItem) throws IOException {
        PercentilesAggregationItem result = new PercentilesAggregationItem();
        result.setKey(percentilesAggregationItem.getKey());
        result.setValue(SearchVariantType.forceConvertToDestColumnValue(percentilesAggregationItem.getValue().toByteArray()));
        return result;
    }

}
