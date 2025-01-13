package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.search.ParallelScanRequest;
import com.alicloud.openservices.tablestore.model.search.ScanQuery;
import com.alicloud.openservices.tablestore.model.search.query.QueryBuilders;
import com.alicloud.openservices.tablestore.model.search.query.WildcardQuery;
import org.junit.Test;

import static com.alicloud.openservices.tablestore.core.protocol.SearchProtocolBuilder.buildParallelScanRequest;
import static com.alicloud.openservices.tablestore.core.protocol.SearchProtocolBuilder.buildScanQuery;
import static org.junit.Assert.assertEquals;

public class ScanQueryBuilderTest extends BaseSearchTest {

    @Test
    public void testBuildParallelScanRequest() {
        WildcardQuery query = new WildcardQuery();
        query.setFieldName("FieldName");
        query.setValue("FieldValue");
        ScanQuery scanQuery = ScanQuery
            .newBuilder()
            .query(QueryBuilders.wildcard("FieldName", "FieldValue"))
            .limit(13)
            .currentParallelId(12)
            .maxParallel(1)
            .aliveTimeInSeconds(98)
            .build();
        ParallelScanRequest sq1 = ParallelScanRequest
            .newBuilder()
            .scanQuery(scanQuery)
            .build();
        Search.ParallelScanRequest request1 = buildParallelScanRequest(sq1);


        Search.Query qbQuery = SearchQueryBuilder.buildQuery(query);
        Search.ScanQuery.Builder builder = Search.ScanQuery.newBuilder();
        builder.setQuery(qbQuery);
        builder.setCurrentParallelId(12);
        builder.setMaxParallel(1);
        builder.setLimit(13);
        builder.setAliveTime(98);
        Search.ScanQuery sq2 = builder.build();
        Search.ParallelScanRequest.Builder builder1 = Search.ParallelScanRequest.newBuilder();
        builder1.setScanQuery(sq2);
        Search.ParallelScanRequest request2 = builder1.build();

        assertEquals(request1.toByteString(), request2.toByteString());

    }

    @Test
    public void testBuildScanQuery() {
        WildcardQuery query = new WildcardQuery();
        query.setFieldName("FieldName");
        query.setValue("FieldValue");
        ScanQuery scanQuery = ScanQuery
            .newBuilder()
            .query(QueryBuilders.wildcard("FieldName", "FieldValue"))
            .limit(13)
            .currentParallelId(12)
            .maxParallel(1)
            .aliveTimeInSeconds(98)
            .build();
        Search.ScanQuery sq1 = buildScanQuery(scanQuery);

        Search.Query qbQuery = SearchQueryBuilder.buildQuery(query);
        Search.ScanQuery.Builder builder = Search.ScanQuery.newBuilder();
        builder.setQuery(qbQuery);
        builder.setCurrentParallelId(12);
        builder.setMaxParallel(1);
        builder.setLimit(13);
        builder.setAliveTime(98);
        Search.ScanQuery sq2 = builder.build();

        assertEquals(sq1.toByteString(), sq2.toByteString());


    }
}
