package examples;

import com.alicloud.openservices.tablestore.core.protocol.SearchProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.SearchProtocolParser;
import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.alicloud.openservices.tablestore.core.protocol.SearchQueryParser;
import com.alicloud.openservices.tablestore.model.search.ParallelScanRequest;
import com.alicloud.openservices.tablestore.model.search.ScanQuery;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.QueryBuilders;
import org.junit.Test;

import java.io.IOException;


public class SearchIndexSerializationSample {

    @Test
    public void testQuery() throws IOException {
        // any kind of query
        Query oldQuery = QueryBuilders.bool().must(QueryBuilders.range("field_a").greaterThan(10.4)).build();
        // serialization
        byte[] bytes = SearchQueryBuilder.buildQueryToBytes(oldQuery);
        // deserialization
        Query newQuery = SearchQueryParser.toQuery(bytes);
    }

    @Test
    public void testSearchQuery() throws IOException {
        SearchQuery oldSearchQuery = new SearchQuery();
        // serialization
        byte[] bytes = SearchProtocolBuilder.buildSearchQueryToBytes(oldSearchQuery);
        // deserialization
        SearchQuery newSearchQuery = SearchProtocolParser.toSearchQuery(bytes);
    }

    @Test
    public void testSearchRequest() throws IOException {
        SearchRequest oldSearchRequest = new SearchRequest();
        // serialization
        byte[] bytes = SearchProtocolBuilder.buildSearchRequestToBytes(oldSearchRequest);
        // deserialization
        SearchRequest newSearchRequest = SearchProtocolParser.toSearchRequest(bytes);
    }

    @Test
    public void testScanQuery() throws IOException {
        ScanQuery oldScanQuery = new ScanQuery();
        // serialization
        byte[] bytes = SearchProtocolBuilder.buildScanQueryToBytes(oldScanQuery);
        // deserialization
        ScanQuery newScanQuery = SearchProtocolParser.toScanQuery(bytes);
    }

    @Test
    public void testParallelScanRequest() throws IOException {
        ParallelScanRequest oldParallelScanRequest = new ParallelScanRequest();
        // serialization
        byte[] bytes = SearchProtocolBuilder.buildParallelScanRequestToBytes(oldParallelScanRequest);
        // deserialization
        ParallelScanRequest newParallelScanRequest = SearchProtocolParser.toParallelScanRequest(bytes);
    }
}
