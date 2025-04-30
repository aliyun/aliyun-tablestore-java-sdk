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
import com.alicloud.openservices.tablestore.model.search.query.KnnVectorQuery;
import com.alicloud.openservices.tablestore.model.search.query.MatchAllQuery;
import com.alicloud.openservices.tablestore.model.search.query.QueryBuilders;
import com.alicloud.openservices.tablestore.model.search.sort.ScoreSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.vector.VectorDataType;
import com.alicloud.openservices.tablestore.model.search.vector.VectorMetricType;
import com.alicloud.openservices.tablestore.model.search.vector.VectorOptions;

import java.util.Arrays;
import java.util.Collections;

public class SearchKnnVectorSample {

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

            // Wait for data to sync to SearchIndex
            waitUntilAllDataSync(client, INDEX_NAME, 2);

            // Use vector query
            knnVectorQuery(client);

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

        int timeToLive = -1; // The expiration time of the data, in seconds. -1 means never expires. For example, if you set the expiration time to one year, it would be 365 * 24 * 3600.
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
                new FieldSchema("Col_Vector", FieldType.VECTOR).setIndex(true).setVectorOptions(new VectorOptions(VectorDataType.FLOAT_32, 4, VectorMetricType.COSINE))
        ));
        request.setIndexSchema(indexSchema);
        client.createSearchIndex(request);
    }

    private static void putRow(SyncClient client) {
        String[] keywords = {"hangzhou", "beijing", "shanghai", "hangzhou shanghai", "hangzhou beijing shanghai"};
        for (int i = 0; i < 5; i++) {
            // Construct the primary key
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("pk1" + i));
            PrimaryKey primaryKey = primaryKeyBuilder.build();

            RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);

            // Add some property columns
            rowPutChange.addColumn("Col_Keyword", ColumnValue.fromString(keywords[i]));
            // Write vector field
            rowPutChange.addColumn("Col_Vector", ColumnValue.fromString("[0.1, 1,2, 0.6, " + i + "]"));
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

    private static void knnVectorQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        KnnVectorQuery query = new KnnVectorQuery();
        query.setFieldName("Col_Vector");
        query.setTopK(100);
        query.setMinScore(0.1f);
        query.setNumCandidates(200);
        query.setFloat32QueryVector(new float[]{0.1f, 0.2f, 0.3f, 0.4f});

        // The most similar vectors need to meet: Col_Keyword=hangzhou, and users can freely combine other queries.
        query.setFilter(QueryBuilders.term("Col_Keyword", "hangzhou"));

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
