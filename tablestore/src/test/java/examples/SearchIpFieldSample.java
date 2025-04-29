package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.alicloud.openservices.tablestore.model.search.CreateSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.IndexSchema;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchResponse;
import com.alicloud.openservices.tablestore.model.search.query.MatchAllQuery;
import com.alicloud.openservices.tablestore.model.search.query.QueryBuilders;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.model.search.sort.ScoreSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;

import java.util.Arrays;
import java.util.Collections;

/**
 * Multi-index supports fields of IP type
 */
public class SearchIpFieldSample {

    private static final String TABLE_NAME = "search_index_sample_table";
    private static final String INDEX_NAME = "test_index";
    private static final String PRIMARY_KEY_NAME_1 = "pk1";

    public static void main(String[] args) {

        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        SyncClient client = new SyncClient(endPoint, accessId, accessKey, instanceName);

        try {
            // Create table
            createTable(client);

            System.out.println("create table succeeded.");

            // Create a SearchIndex
            createSearchIndex(client);
            System.out.println("create search index succeeded.");

            // putRow writes multiple rows of data
            putRow(client);
            System.out.println("put row succeeded.");

            // Wait for the data to be synchronized to SearchIndex
            waitUntilAllDataSync(client, INDEX_NAME, 5);

            // Query IP addresses using CIDR notation
            searchIpSegmentUsingCIDR(client);

            // Query IP addresses through RangeQuery, this method has the same effect as CIDR notation query.
            searchIpSegmentUsingRangeQuery(client);

            // Exact query for IP address
            searchExactIp(client);

        } catch (TableStoreException e) {
            System.err.println("operation failed, detail: " + e.getMessage());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("request failed, detail: " + e.getMessage());
        }
        client.shutdown();
    }

    private static void createTable(SyncClient client) {
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME_1, PrimaryKeyType.STRING));

        int timeToLive = -1; // The expiration time of the data, in seconds. -1 means never expires. For example, if the expiration time is set to one year, it would be 365 * 24 * 3600.
        int maxVersions = 1; // The maximum number of versions to save, setting it to 1 means that at most one version is saved for each column (saving the latest version).

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        client.createTable(request);
    }

    private static void createSearchIndex(SyncClient client) {
        CreateSearchIndexRequest request = new CreateSearchIndexRequest();
        request.setTableName(TABLE_NAME);
        request.setIndexName(INDEX_NAME);
        IndexSchema indexSchema = new IndexSchema();
        indexSchema.setFieldSchemas(Arrays.asList(
            new FieldSchema("Col_Keyword", FieldType.KEYWORD).setIndex(true).setEnableSortAndAgg(true),
            new FieldSchema("Col_Ip", FieldType.IP).setIndex(true).setEnableSortAndAgg(true)
        ));
        request.setIndexSchema(indexSchema);
        client.createSearchIndex(request);
    }

    private static void putRow(SyncClient client) {
        String[] keywords = { "Router", "Phone", "PC1", "PC2", "Home Bot" };
        for (int i = 0; i < 5; i++) {
            // Construct the primary key
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("pk1" + i));
            PrimaryKey primaryKey = primaryKeyBuilder.build();

            RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);

            // Add some property columns
            rowPutChange.addColumn("Col_Keyword", ColumnValue.fromString(keywords[i]));
            // Write the IP field
            rowPutChange.addColumn("Col_Ip", ColumnValue.fromString("192.168.1." + i));
            client.putRow(new PutRowRequest(rowPutChange));
        }
    }

    private static void waitUntilAllDataSync(SyncClient client, String indexName, long expectTotalHit) {
        long begin = System.currentTimeMillis();
        while (true) {
            SearchQuery searchQuery = new SearchQuery();
            searchQuery.setQuery(new MatchAllQuery());
            searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
            SearchRequest searchRequest = new SearchRequest(TABLE_NAME, indexName, searchQuery);
            SearchResponse resp = client.search(searchRequest);
            if (resp.getTotalCount() == expectTotalHit) {
                break;
            }
            if (System.currentTimeMillis() - begin > 150 * 1000) {
                throw new RuntimeException("Wait timeout.");
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Query IP address ranges using CIDR notation
     * When using CIDR notation to query IP types, the query must use term/terms Query
     */
    private static void searchIpSegmentUsingCIDR(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        // TermQuery supports CIDR notation for querying IP types. For example: to query all devices under the 192.168.1 subnet, you can use the following method.
        TermQuery query = QueryBuilders.term("Col_Ip", "192.168.1.1/24").build();
        searchQuery.setQuery(query);
        searchQuery.setLimit(100);
        searchQuery.setSort(new Sort(Collections.singletonList(new ScoreSort()))); // Sort by score
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setReturnAll(true);
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Query IP address ranges using RangeQuery
     * When using RangeQuery to query IP types, CIDR notation cannot be used.
     */
    private static void searchIpSegmentUsingRangeQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        // Use RangeQuery to query IP address ranges, this usage has the same effect as QueryBuilders.term("Col_Ip", "192.168.1.1/24").
        RangeQuery query = QueryBuilders.range("Col_Ip").greaterThanOrEqual("192.168.1.1").lessThanOrEqual("192.168.1.255").build();
        searchQuery.setQuery(query);
        searchQuery.setLimit(100);
        searchQuery.setSort(new Sort(Collections.singletonList(new ScoreSort()))); // Sort by score
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setReturnAll(true);
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Exact IP address query
     * When performing an exact IP address query, use TermQuery.
     */
    private static void searchExactIp(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        TermQuery query = QueryBuilders.term("Col_Ip", "192.168.1.1").build();
        searchQuery.setQuery(query);
        searchQuery.setLimit(100);
        searchQuery.setSort(new Sort(Collections.singletonList(new ScoreSort()))); // Sort by score
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setReturnAll(true);
        searchRequest.setColumnsToGet(columnsToGet);
        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }
}
