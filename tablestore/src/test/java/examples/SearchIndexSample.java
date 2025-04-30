package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.utils.StringUtils;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.iterator.RowIterator;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.search.SearchRequest.ColumnsToGet;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilders;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationResults;
import com.alicloud.openservices.tablestore.model.search.agg.PercentilesAggregationItem;
import com.alicloud.openservices.tablestore.model.search.agg.PercentilesAggregationResult;
import com.alicloud.openservices.tablestore.model.search.analysis.AnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SplitAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByBuilders;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByDateHistogramItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFieldResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFieldResultItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFilterResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFilterResultItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByGeoGridResultItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByHistogramItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByHistogramResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByRangeResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByRangeResultItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByComposite;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByCompositeResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByCompositeResultItem;
import com.alicloud.openservices.tablestore.model.search.highlight.Highlight;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightField;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightFragmentOrder;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightParameter;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.alicloud.openservices.tablestore.model.search.sort.DocSort;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.ScoreSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SearchIndexSample {

    /**
     * In this example, a table is created named "search_index_sample_table" with two primary keys, pk1 and pk2.
     * A SearchIndex is created for this table, then the SearchIndex under the table is listed, and the information of the SearchIndex is queried.
     * Write a few records into the table and query them through several search queries.
     */
    private static final String TABLE_NAME = "search_index_sample_table";
    private static final String INDEX_NAME = "test_index";
    private static final String INDEX_NAME_SCHEMA_MODIFIED = "test_index_reindex";
    private static final String PRIMARY_KEY_NAME_1 = "pk1";
    private static final String PRIMARY_KEY_NAME_2 = "pk2";

    public static void main(String[] args) {

        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        SyncClient client = new SyncClient(endPoint, accessId, accessKey,
            instanceName);

        try {
            // Create table
            createTable(client);

            System.out.println("create table succeeded.");

            // Create a SearchIndex
            createSearchIndex(client);
            System.out.println("create search index succeeded.");

            // List all SearchIndexes under the table
            System.out.println(System.currentTimeMillis());
            List<SearchIndexInfo> indexInfos = listSearchIndex(client);
            System.out.println("list search index succeeded, indexInfo: \n" + indexInfos);
            System.out.println(System.currentTimeMillis());

            // Query the description information of SearchIndex
            DescribeSearchIndexResponse describeSearchIndexResponse = describeSearchIndex(client);
            System.out.println("describe search index succeeded, response: \n" + describeSearchIndexResponse.jsonize());

            // Wait for the table to load and the searchIndex to initialize.
            try {
                System.out.println("sleeping...");
                Thread.sleep(20 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // putRow writes multiple rows of data
            putRow(client);
            System.out.println("put row succeeded.");

            // Wait for the data to sync to SearchIndex
            waitUntilAllDataSync(client, INDEX_NAME, 7);

            // Use matchAllQuery to query the total number of data rows
            System.out.println("MatchAllQuery...");
            matchAllQuery(client);

            // Use MatchQuery to query data
            System.out.println("MatchQuery...");
            matchQuery(client);

            // Use MatchQuery to query data and highlight keywords
            System.out.println("MatchQuery with Highlighting...");
            matchQueryWithHighlighting(client);

            // Use RangeQuery to query data and sort it.
            System.out.println("RangeQuery...");
            rangeQuery(client);

            // Use MatchPhraseQuery to query data
            System.out.println("MatchPhraseQuery...");
            matchPhraseQuery(client);

            // Use PrefixQuery to query data
            System.out.println("PrefixQuery...");
            prefixQuery(client);

            // Use WildcardQuery to query data
            System.out.println("WildcardQuery...");
            wildcardQuery(client);

            // Use SuffixQuery to query data
            System.out.println("SuffixQuery...");
            suffixQuery(client);

            // Use TermQuery to query data
            System.out.println("TermQuery...");
            termQuery(client);

            // Use BoolQuery to query data
            System.out.println("BoolQuery...");
            boolQuery(client);

            // Use ExistsQuery to query data
            System.out.println("ExistsQuery...");
            existsQuery(client);

            // Query data using groupByHistogram
            System.out.println("groupByHistogram...");
            groupByHistogram(client);

            // Use groupByDateHistogram to query data
            System.out.println("groupByDateHistogram...");
            groupByDateHistogram(client);

            // Use groupByGeoGrid to query data
            System.out.println("groupByGeoGrid...");
            groupByGeoGrid(client);

            // Use groupByComposite to query data
            System.out.println("groupByComposite...");
            groupByComposite(client);

            // Use percentilesAggLong to query data
            System.out.println("percentilesAggLong(client);...");
            percentilesAggLong(client);

            // Dynamically modify schema
            System.out.println("DynamicModifySchema...");
            dynamicModifySchema(client);

        } catch (TableStoreException e) {
            System.err.println("operation failed, detail: " + e.getMessage());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("request failed, detail: " + e.getMessage());
        } finally {
            //             // For security reasons, indexes and tables cannot be deleted by default here. If deletion is required, users need to manually enable it.
            //            try {
            //                 // Delete SearchIndex
            //                deleteSearchIndex(client);
            //            } catch (Exception ex) {
            //                ex.printStackTrace();
            //            }
            //            deleteTable(client);
        }
        client.shutdown();
    }

    private static void createTable(SyncClient client) {
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME_1, PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME_2, PrimaryKeyType.INTEGER));

        int timeToLive = -1; // The expiration time of the data, in seconds, -1 means never expires. If the expiration time is set to one year, it would be 365 * 24 * 3600.
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
            new FieldSchema("Col_Fuzzy_Keyword", FieldType.FUZZY_KEYWORD).setIndex(true).setEnableSortAndAgg(true),
            new FieldSchema("Col_Ip", FieldType.IP).setIndex(true).setEnableSortAndAgg(true),
            new FieldSchema("Col_Long", FieldType.LONG).setIndex(true).setEnableSortAndAgg(true),
            new FieldSchema("Col_Long_sec", FieldType.LONG).setIndex(true).setEnableSortAndAgg(true),
            new FieldSchema("Col_Double", FieldType.DOUBLE).setIndex(true).setEnableSortAndAgg(true),
            new FieldSchema("Col_Text", FieldType.TEXT).setIndex(true).setEnableHighlighting(true),
            new FieldSchema("Col_Date", FieldType.DATE).setIndex(true).setDateFormats(Collections.singletonList("yyyy-MM-dd HH:mm:ss")),
            new FieldSchema("Col_Nested", FieldType.NESTED).setIndex(true).setSubFieldSchemas(Arrays.asList(
                new FieldSchema("Level1_Col1_Text", FieldType.TEXT).setIndex(true).setStore(true).setEnableHighlighting(true),
                new FieldSchema("Level1_Col2_Nested", FieldType.NESTED).setIndex(true)
                    .setSubFieldSchemas(Collections.singletonList(new FieldSchema("Level2_Col1_Text", FieldType.TEXT).setIndex(true).setStore(true).setEnableHighlighting(true)))
            )),
            new FieldSchema("Col_Json", FieldType.JSON).setJsonType(JsonType.FLATTEN).setSubFieldSchemas(Arrays.asList(
                new FieldSchema("Level1_Col1_Keyword", FieldType.TEXT).setIndex(true).setEnableSortAndAgg(true),
                new FieldSchema("Level1_Col2_Long", FieldType.LONG).setIndex(true).setEnableSortAndAgg(true),
                new FieldSchema("Level1_Col3_NestedJson", FieldType.JSON).setJsonType(JsonType.NESTED)
                    .setSubFieldSchemas(Collections.singletonList(new FieldSchema("Level2_Col1_Keyword", FieldType.KEYWORD).setIndex(true).setEnableSortAndAgg(true)))
            ))
        ));
        request.setIndexSchema(indexSchema);
        client.createSearchIndex(request);
    }

    private static void createSearchIndexWithAnalyzer(SyncClient client) {
        FieldSchema.Analyzer analyzer = FieldSchema.Analyzer.Split;
        AnalyzerParameter analyzerParameter = new SplitAnalyzerParameter("-");

        CreateSearchIndexRequest request = new CreateSearchIndexRequest();
        request.setTableName(TABLE_NAME);
        request.setIndexName(INDEX_NAME);
        IndexSchema indexSchema = new IndexSchema();
        indexSchema.setFieldSchemas(Arrays.asList(
            new FieldSchema("Col_Text", FieldType.TEXT).setIndex(true).setAnalyzer(analyzer)
                .setAnalyzerParameter(analyzerParameter).setEnableHighlighting(true)));
        request.setIndexSchema(indexSchema);
        client.createSearchIndex(request);
    }

    private static void createSearchIndexWithIndexSort(SyncClient client) {
        CreateSearchIndexRequest request = new CreateSearchIndexRequest();
        request.setTableName(TABLE_NAME);
        request.setIndexName(INDEX_NAME);
        IndexSchema indexSchema = new IndexSchema();
        indexSchema.setFieldSchemas(Arrays.asList(
            new FieldSchema("Col_Keyword", FieldType.KEYWORD).setIndex(true).setEnableSortAndAgg(true),
            new FieldSchema("Col_Long", FieldType.LONG).setIndex(true).setEnableSortAndAgg(true),
            new FieldSchema("Col_Double", FieldType.DOUBLE).setIndex(true).setEnableSortAndAgg(true),
            new FieldSchema("Col_Text", FieldType.TEXT).setIndex(true),
            new FieldSchema("Timestamp", FieldType.LONG).setIndex(true).setEnableSortAndAgg(true)));
        indexSchema.setIndexSort(new Sort(
            Arrays.<Sort.Sorter>asList(new FieldSort("Timestamp", SortOrder.ASC))));
        request.setIndexSchema(indexSchema);
        client.createSearchIndex(request);
    }

    /**
     * Use Token for pagination.
     **/
    private static void readMoreRowsWithToken(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(new MatchAllQuery());
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT); // You need to set GetTotalCount to true in order to return the total number of rows of data that satisfy the conditions.
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);
        SearchResponse resp = client.search(searchRequest);
        if (!resp.isAllSuccess()) {
            throw new RuntimeException("not all success");
        }
        List<Row> rows = resp.getRows();
        while (resp.getNextToken() != null) {
            searchRequest.getSearchQuery().setToken(resp.getNextToken());
            resp = client.search(searchRequest);
            if (!resp.isAllSuccess()) {
                throw new RuntimeException("not all success");
            }
            rows.addAll(resp.getRows());
        }
        System.out.println("RowSize: " + rows.size());
        System.out.println("TotalCount: " + resp.getTotalCount());
    }

    private static List<SearchIndexInfo> listSearchIndex(SyncClient client) {
        ListSearchIndexRequest request = new ListSearchIndexRequest();
        request.setTableName(TABLE_NAME);
        return client.listSearchIndex(request).getIndexInfos();
    }

    private static DescribeSearchIndexResponse describeSearchIndex(SyncClient client) {
        DescribeSearchIndexRequest request = new DescribeSearchIndexRequest();
        request.setTableName(TABLE_NAME);
        request.setIndexName(INDEX_NAME);

        // If includeSyncStat is set to false, the SyncStat information will not be included in the returned result. Not setting it or setting it to true will both return SyncStat normally.
        // request.setIncludeSyncStat(false);
        DescribeSearchIndexResponse response = client.describeSearchIndex(request);
        System.out.println(response.jsonize());
        return response;
    }

    private static void deleteSearchIndex(SyncClient client) {
        DeleteSearchIndexRequest request = new DeleteSearchIndexRequest();
        request.setTableName(TABLE_NAME);
        request.setIndexName(INDEX_NAME);
        client.deleteSearchIndex(request);

        request.setIndexName(INDEX_NAME_SCHEMA_MODIFIED);
        client.deleteSearchIndex(request);
    }

    private static void deleteTable(SyncClient client) {
        DeleteTableRequest request = new DeleteTableRequest(TABLE_NAME);
        client.deleteTable(request);
    }

    private static void putRow(SyncClient client) {
        String[] keywords = {"hangzhou", "beijing", "shanghai", "hangzhou shanghai", "hangzhou beijing shanghai"};
        String[] fuzzyKeywords = keywords.clone();
        long[] longValues = {1, 2, 3, 4, 5, 6, 7};
        double[] doubleValues = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
        String[] dates = {"2017-05-01 00:00:01", "2017-05-03 00:00:01", "2017-05-10 00:00:01", "2017-05-15 00:00:01",
                "2017-05-15 12:10:01", "2017-05-16 00:00:01", "2017-05-20 00:00:01"};
        for (int i = 0; i < 5; i++) {
            // Construct the Nested attribute column
            String stringBuilder = "[{" +
                "\"Level1_Col1_Text\":\"" + keywords[i] + " " + i + "_1" + "\"," +
                "\"Level1_Col2_Nested\":" + "[{" +
                "\"Level2_Col1_Text\":\"" + keywords[i] + " " + i + "_1" + "\"" + "}]}," +
                "{" +
                "\"Level1_Col1_Text\":\"" + keywords[i] + " " + i + "_2" + "\"," +
                "\"Level1_Col2_Nested\":" + "[{" +
                "\"Level2_Col1_Text\":\"" + keywords[i] + " " + i + "_2" + "\"" + "}]}]";

            // Construct the primary key
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("sample"));
            primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(i));
            PrimaryKey primaryKey = primaryKeyBuilder.build();

            RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);

            // Add some property columns
            rowPutChange.addColumn("Col_Keyword", ColumnValue.fromString(keywords[i]));
            rowPutChange.addColumn("Col_Fuzzy_Keyword", ColumnValue.fromString(fuzzyKeywords[i]));
            rowPutChange.addColumn("Col_Long", ColumnValue.fromLong(longValues[i]));
            rowPutChange.addColumn("Col_Long_sec", ColumnValue.fromLong(longValues[i]));
            rowPutChange.addColumn("Col_Double", ColumnValue.fromDouble(doubleValues[i]));
            rowPutChange.addColumn("Col_Text", ColumnValue.fromString(keywords[i]));
            rowPutChange.addColumn("Col_Boolean", ColumnValue.fromBoolean(i % 2 == 0 ? true : false));
            rowPutChange.addColumn("Col_Nested", ColumnValue.fromString(stringBuilder));
            rowPutChange.addColumn("Col_Date", ColumnValue.fromString(dates[i]));
            client.putRow(new PutRowRequest(rowPutChange));
        }
        {   // Construct a row missing the Col_Keyword and Col_Text columns
            // Construct the primary key
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("sample"));
            primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(5));
            PrimaryKey primaryKey = primaryKeyBuilder.build();

            RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);

            // Add some property columns
            rowPutChange.addColumn("Col_Long", ColumnValue.fromLong(longValues[5]));
            rowPutChange.addColumn("Col_Boolean", ColumnValue.fromBoolean(false));
            client.putRow(new PutRowRequest(rowPutChange));
        }
        {   // Construct a row with missing Col_Keyword, Col_long, and Col_Text columns.
            // Construct the primary key
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("sample"));
            primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(6));
            PrimaryKey primaryKey = primaryKeyBuilder.build();

            RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);

            // Add some property columns
            rowPutChange.addColumn("Col_Long_sec", ColumnValue.fromLong(longValues[6]));
            rowPutChange.addColumn("Col_Boolean", ColumnValue.fromBoolean(false));
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
     * Query the total number of rows in the table through MatchAllQuery
     */
    private static void matchAllQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(new MatchAllQuery());
        /**
         * The TotalCount in the MatchAllQuery result can represent the total number of rows in the table.
         * If you only want to retrieve the TotalCount, you can set limit=0, which means no data rows will be returned.
         */
        searchQuery.setLimit(0);
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT); // You need to set GetTotalCount to true to return the total number of rows that satisfy the conditions.
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);
        SearchResponse resp = client.search(searchRequest);
        /**
         * Determine if the returned result is complete. When isAllSuccess is false, it indicates that some nodes may have failed to query, and only partial data is returned.
         */
        if (!resp.isAllSuccess()) {
            System.out.println("NotAllSuccess!");
        }
        System.out.println("IsAllSuccess: " + resp.isAllSuccess());
        System.out.println("TotalCount: " + resp.getTotalCount());
        System.out.println(resp.getRequestId());
    }

    /**
     * Query the data where the values in the Col_Keyword column of the table match "hangzhou", and return the total number of matched rows and some successfully matched rows.
     */
    private static void matchQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        MatchQuery matchQuery = new MatchQuery(); // Set the query type to MatchQuery
        matchQuery.setFieldName("Col_Keyword"); // Set the field to match
        matchQuery.setText("hangzhou"); // Set the value to match
        searchQuery.setQuery(matchQuery);
        searchQuery.setOffset(0); // Set offset to 0
        searchQuery.setLimit(20); // Set the limit to 20, which means a maximum of 20 rows of data will be returned.
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);
        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount());
        System.out.println("Row: " + resp.getRows()); // If columnsToGet is not set, only the primary key will be returned by default.

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // Set to return all columns
        searchRequest.setColumnsToGet(columnsToGet);

        resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Query the values in the Col_Text column of the table that match "hangzhou shanghai",
     * with the matching condition being phrase matching (requires the phrase to match in exact order),
     * and return the total number of matched rows and some successfully matched rows.
     */
    private static void matchPhraseQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery(); // Set the query type to MatchPhraseQuery
        matchPhraseQuery.setFieldName("Col_Text"); // Set the field to match
        matchPhraseQuery.setText("hangzhou shanghai"); // Set the value to match
        searchQuery.setQuery(matchPhraseQuery);
        searchQuery.setOffset(0); // Set the offset to 0
        searchQuery.setLimit(20); // Set the limit to 20, which means a maximum of 20 rows of data will be returned.
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);
        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount());
        System.out.println("Row: " + resp.getRows()); // By default, only the primary key is returned.

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // Set to return all columns
        searchRequest.setColumnsToGet(columnsToGet);

        resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Query the data in the Col_Keyword column of the table that exactly matches "hangzhou".
     */
    private static void termQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        TermQuery termQuery = new TermQuery(); // Set the query type to TermQuery
        termQuery.setFieldName("Col_Keyword"); // Set the field to match
        termQuery.setTerm(ColumnValue.fromString("hangzhou")); // Set the value to match
        searchQuery.setQuery(termQuery);
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // Set to return all columns
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Query the data in the Col_Keyword column of the table that exactly matches "hangzhou" or "shanghai".
     * TermsQuery can be used to query with multiple Terms simultaneously.
     */
    private static void termsQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        TermsQuery termsQuery = new TermsQuery(); // Set the query type to TermQuery
        termsQuery.setFieldName("Col_Keyword"); // Set the field to match
        termsQuery.setTerms(Arrays.asList(ColumnValue.fromString("hangzhou"),
            ColumnValue.fromString("shanghai"))); // Set the value to match
        searchQuery.setQuery(termsQuery);
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // Set to return all columns
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Query the data in the Col_Keyword column of the table where the prefix is "hangzhou".
     */
    private static void prefixQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        PrefixQuery prefixQuery = new PrefixQuery(); // Set the query type to PrefixQuery
        prefixQuery.setFieldName("Col_Keyword");
        prefixQuery.setPrefix("hangzhou");
        searchQuery.setQuery(prefixQuery);
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // Set to return all columns
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Query the data with suffix "hai" in the Col_Fuzzy_Keyword column of the table.
     * Note: Only the Fuzzy_Keyword type supports suffix query SuffixQuery.
     * Note: Compared to the Keyword type, both prefix query PrefixQuery and wildcard query WildcardQuery of the Fuzzy_Keyword type have better performance.
     */
    private static void suffixQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        SuffixQuery suffixQuery = new SuffixQuery(); // Set the query type to SuffixQuery
        suffixQuery.setFieldName("Col_Fuzzy_Keyword");
        suffixQuery.setSuffix("hai");
        searchQuery.setQuery(suffixQuery);
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // Set to return all columns
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Use wildcard query to search for data where the value of the Col_Keyword column in the table matches "hang*u".
     */
    private static void wildcardQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        WildcardQuery wildcardQuery = new WildcardQuery(); // Set the query type to WildcardQuery
        wildcardQuery.setFieldName("Col_Keyword");
        wildcardQuery.setValue("hang*u"); // wildcardQuery supports wildcards
        searchQuery.setQuery(wildcardQuery);
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // Set to return all columns
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);

        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Query the data in the Col_Long column of the table where the value is greater than 3, and sort the results in descending order by the value of the Col_Long column.
     */
    private static void rangeQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        RangeQuery rangeQuery = new RangeQuery(); // Set the query type to RangeQuery
        rangeQuery.setFieldName("Col_Long");  // Set the target field
        rangeQuery.greaterThan(ColumnValue.fromLong(3));  // Set the range condition for this field to be greater than 3.
        searchQuery.setQuery(rangeQuery);
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
        // Set reverse order sorting by the Col_Long column
        FieldSort fieldSort = new FieldSort("Col_Long");
        fieldSort.setOrder(SortOrder.DESC);
        searchQuery.setSort(new Sort(Arrays.asList((Sort.Sorter)fieldSort)));

        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Query all data in the table and reverse sort based on the value of the Col_Long column. If data is missing, use Col_Long_sec data for sorting.
     */
    private static void fieldSortQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(new MatchAllQuery());
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
        // Set reverse order sorting by the Col_Long column
        FieldSort fieldSort = new FieldSort("Col_Long");
        fieldSort.setMissingField("Col_Long_sec");
        fieldSort.setOrder(SortOrder.DESC);
        searchQuery.setSort(new Sort(Arrays.asList((Sort.Sorter)fieldSort)));

        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Col_GeoPoint is of GeoPoint type. This queries the values in the Col_GeoPoint column of the table that fall within a rectangular range with the top-left corner at "10,0" and the bottom-right corner at "0,10".
     */
    public static void geoBoundingBoxQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        GeoBoundingBoxQuery geoBoundingBoxQuery = new GeoBoundingBoxQuery(); // Set the query type to GeoBoundingBoxQuery
        geoBoundingBoxQuery.setFieldName("Col_GeoPoint"); // Set which field's value to compare
        geoBoundingBoxQuery.setTopLeft("10,0"); // Set the top-left corner of the rectangle
        geoBoundingBoxQuery.setBottomRight("0,10"); // Set the bottom-right corner of the rectangle
        searchQuery.setQuery(geoBoundingBoxQuery);
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setColumns(Arrays.asList("Col_GeoPoint"));  // Set to return the Col_GeoPoint column
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Query the data where the values in the Col_GeoPoint column are within a certain distance from the center point.
     */
    public static void geoDistanceQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        GeoDistanceQuery geoDistanceQuery = new GeoDistanceQuery();  // Set the query type to GeoDistanceQuery
        geoDistanceQuery.setFieldName("Col_GeoPoint");
        geoDistanceQuery.setCenterPoint("5,5"); // Set the center point
        geoDistanceQuery.setDistanceInMeter(10000); // Set the distance condition to the center point, not exceeding 10,000 meters.
        searchQuery.setQuery(geoDistanceQuery);
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setColumns(Arrays.asList("Col_GeoPoint"));  // Set to return the Col_GeoPoint column.
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Query the data where the values of the Col_GeoPoint column in the table are within a given polygon range.
     */
    public static void geoPolygonQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        GeoPolygonQuery geoPolygonQuery = new GeoPolygonQuery();  // Set the query type to GeoPolygonQuery
        geoPolygonQuery.setFieldName("Col_GeoPoint");
        geoPolygonQuery.setPoints(Arrays.asList("0,0", "5,5", "5,0")); // Set the vertices of the polygon
        searchQuery.setQuery(geoPolygonQuery);
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setColumns(Arrays.asList("Col_GeoPoint"));  // Set to return the Col_GeoPoint column
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Perform a compound condition query through BoolQuery.
     */
    public static void boolQuery(SyncClient client) {
        /*
         * Query condition one: RangeQuery, the value of the Col_Long column must be greater than 3.
         */
        RangeQuery rangeQuery = new RangeQuery();
        rangeQuery.setFieldName("Col_Long");
        rangeQuery.greaterThan(ColumnValue.fromLong(3));

        /*
         * Query condition two: MatchQuery, the value of the Col_Keyword column should match "hangzhou"
         */
        MatchQuery matchQuery = new MatchQuery(); // Set the query type to MatchQuery
        matchQuery.setFieldName("Col_Keyword"); // Set the field to match
        matchQuery.setText("hangzhou"); // Set the value to match

        SearchQuery searchQuery = new SearchQuery();
        {
            /*
             * Constructs a BoolQuery, setting the query condition to require both "Condition One" and "Condition Two" to be satisfied simultaneously.
             */
            BoolQuery boolQuery = new BoolQuery();
            boolQuery.setMustQueries(Arrays.asList(rangeQuery, matchQuery));
            searchQuery.setQuery(boolQuery);
            searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
            SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);
            SearchResponse resp = client.search(searchRequest);
            System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
            System.out.println("Row: " + resp.getRows());
        }

        {
            /*
             * Construct a BoolQuery, set the query condition to satisfy at least one of "condition one" and "condition two"
             */
            BoolQuery boolQuery = new BoolQuery();
            boolQuery.setShouldQueries(Arrays.asList(rangeQuery, matchQuery));
            boolQuery.setMinimumShouldMatch(1); // Set to meet at least one condition
            searchQuery.setQuery(boolQuery);
            searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
            SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);
            SearchResponse resp = client.search(searchRequest);
            System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
            System.out.println("Row: " + resp.getRows());
        }
    }

    public static void existsQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        ExistsQuery existsQuery = new ExistsQuery();
        existsQuery.setFieldName("Col_Keyword");
        searchQuery.setQuery(existsQuery);
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // Set to return all columns
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // The total number of rows matched, not the number of rows returned
        System.out.println("Row: " + resp.getRows());
    }

    public static void dynamicModifySchema(SyncClient client) {
        // Step 1. Create "Rebuild Index"
        // If you need to delete the index column Col_Long of the source index and add a new column Col_Boolean, you can rebuild the index as follows:
        CreateSearchIndexRequest request = new CreateSearchIndexRequest();
        request.setTableName(TABLE_NAME);
        request.setIndexName(INDEX_NAME_SCHEMA_MODIFIED); // Rebuild index: index after schema modification
        request.setSourceIndexName(INDEX_NAME); // Source index: the index with modified schema

        IndexSchema indexSchema = new IndexSchema();
        indexSchema.setFieldSchemas(Arrays.asList(
                new FieldSchema("Col_Keyword", FieldType.KEYWORD).setIndex(true).setEnableSortAndAgg(true),
                new FieldSchema("Col_Long", FieldType.LONG).setIndex(true).setEnableSortAndAgg(true),
                // new FieldSchema("Col_Text", FieldType.TEXT).setIndex(true);         // Delete the index column
                new FieldSchema("Col_Boolean", FieldType.BOOLEAN).setIndex(true)  // Add index column
        ));
        request.setIndexSchema(indexSchema);
        client.createSearchIndex(request);

        // Step 2: Wait for the "rebuild index" data synchronization, which will go through two phases: "full synchronization" and "incremental synchronization".
        waitUntilAllDataSync(client, INDEX_NAME, 6);

        //step 3. AB test
        {   // Set the query traffic weight: source index 80%, rebuilt index 80%
            UpdateSearchIndexRequest updateSearchIndexRequest = new UpdateSearchIndexRequest(TABLE_NAME, INDEX_NAME,
                    Arrays.asList(
                            new QueryFlowWeight(INDEX_NAME, 80),
                            new QueryFlowWeight(INDEX_NAME_SCHEMA_MODIFIED, 20)
                    ));
            client.updateSearchIndex(updateSearchIndexRequest);

            // Check if the weight setting is successful
            DescribeSearchIndexRequest describeSearchIndexRequest = new DescribeSearchIndexRequest();
            describeSearchIndexRequest.setTableName(TABLE_NAME);
            describeSearchIndexRequest.setIndexName(INDEX_NAME);
            DescribeSearchIndexResponse describeSearchIndexResponse = client.describeSearchIndex(describeSearchIndexRequest);
            System.out.println("describe response: " + describeSearchIndexResponse.jsonize());

            // Wait for the weight setting to take effect
            try {
                Thread.sleep(50000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // At this point, 20% of the queries will be handled by the rebuilt index. Gradually increasing this ratio can switch the query traffic to the rebuilt index in a controlled manner.
            int hitOrigin = 0;
            int hitTotal = 100;
            for (int i = 0; i < hitTotal; ++i) {
                SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME,
                        SearchQuery.newBuilder()
                                .query(QueryBuilders.exists("Col_Boolean"))
                                .getTotalCount(true).build());
                SearchResponse resp = client.search(searchRequest);
                if (resp.getTotalCount() == 0) {
                    ++ hitOrigin;
                }
            }
            System.out.println("hit origin: " + hitOrigin + ", hit modified: " + (hitTotal - hitOrigin));
        }

        {   // Set the weight of query traffic: source index 20%, rebuilt index 80%
            UpdateSearchIndexRequest updateSearchIndexRequest = new UpdateSearchIndexRequest(TABLE_NAME, INDEX_NAME,
                    Arrays.asList(
                            new QueryFlowWeight(INDEX_NAME, 20),
                            new QueryFlowWeight(INDEX_NAME_SCHEMA_MODIFIED, 80)
                    ));
            client.updateSearchIndex(updateSearchIndexRequest);

            // Check if the weight setting is successful
            DescribeSearchIndexRequest describeSearchIndexRequest = new DescribeSearchIndexRequest();
            describeSearchIndexRequest.setTableName(TABLE_NAME);
            describeSearchIndexRequest.setIndexName(INDEX_NAME);
            DescribeSearchIndexResponse describeSearchIndexResponse = client.describeSearchIndex(describeSearchIndexRequest);
            System.out.println("describe response: " + describeSearchIndexResponse.jsonize());

            // Wait for the weight setting to take effect
            try {
                Thread.sleep(50000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // At this point, 80% of the queries will be handled by the rebuilt index.
            int hitOrigin = 0;
            int hitTotal = 100;
            for (int i = 0; i < hitTotal; ++i) {
                SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME,
                        SearchQuery.newBuilder()
                                .query(QueryBuilders.exists("Col_Boolean"))
                                .getTotalCount(true).build());
                SearchResponse resp = client.search(searchRequest);
                if (resp.getTotalCount() == 0) {
                    ++ hitOrigin;
                }
            }
            System.out.println("hit origin: " + hitOrigin + ", hit modified: " + (hitTotal - hitOrigin));
        }

        {   // Set the weight of query traffic: source index 20%, rebuilt index 80%
            UpdateSearchIndexRequest updateSearchIndexRequest = new UpdateSearchIndexRequest(TABLE_NAME, INDEX_NAME,
                    Arrays.asList(
                            new QueryFlowWeight(INDEX_NAME, 0),
                            new QueryFlowWeight(INDEX_NAME_SCHEMA_MODIFIED, 100)
                    ));
            client.updateSearchIndex(updateSearchIndexRequest);

            // Check if the weight setting is successful
            DescribeSearchIndexRequest describeSearchIndexRequest = new DescribeSearchIndexRequest();
            describeSearchIndexRequest.setTableName(TABLE_NAME);
            describeSearchIndexRequest.setIndexName(INDEX_NAME);
            DescribeSearchIndexResponse describeSearchIndexResponse = client.describeSearchIndex(describeSearchIndexRequest);
            System.out.println("describe response: " + describeSearchIndexResponse.jsonize());

            // Wait for the weight setting to take effect
            try {
                Thread.sleep(50000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // At this point, all query traffic is redirected to the rebuilt index.
            int hitOrigin = 0;
            int hitTotal = 100;
            for (int i = 0; i < hitTotal; ++i) {
                SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME,
                        SearchQuery.newBuilder()
                                .query(QueryBuilders.exists("Col_Boolean"))
                                .getTotalCount(true).build());
                SearchResponse resp = client.search(searchRequest);
                if (resp.getTotalCount() == 0) {
                    ++ hitOrigin;
                }
            }
            System.out.println("hit origin: " + hitOrigin + ", hit modified: " + (hitTotal - hitOrigin));
        }

        // Step 4. After a sufficient period of A/B testing, you can switch the index.
        UpdateSearchIndexRequest switchRequest = new UpdateSearchIndexRequest(TABLE_NAME, INDEX_NAME, INDEX_NAME_SCHEMA_MODIFIED);
        client.updateSearchIndex(switchRequest);

//        // If issues are found, there is still a chance to switch back.
//        switchRequest = new UpdateSearchIndexRequest(TABLE_NAME, INDEX_NAME, INDEX_NAME_SCHEMA_MODIFIED);
//        client.updateSearchIndex(switchRequest);

        // Step 5. After a period of silence, the index before modification can be deleted.
        DeleteSearchIndexRequest deleteRequest = new DeleteSearchIndexRequest();
        deleteRequest.setTableName(TABLE_NAME);
        deleteRequest.setIndexName(INDEX_NAME_SCHEMA_MODIFIED);
        client.deleteSearchIndex(deleteRequest);
    }

    /**
     * Find the minimum value of a specific field in the results of MatchQuery.
     */
    public static void minAgg(SyncClient client) {
        SearchRequest searchRequest = SearchRequest.newBuilder()
            .tableName(INDEX_NAME)
            .indexName(TABLE_NAME)
            .returnAllColumns(true)
            .searchQuery(
                SearchQuery.newBuilder()
                    .query(QueryBuilders.match("fieldName1", "hello"))
                    .limit(10)
                    .getTotalCount(true)
                    .addAggregation(AggregationBuilders.min("SomeName1", "fieldName2"))
                    .build())
            .build();
        SearchResponse resp = client.search(searchRequest);
        // Use the result
        System.out.println(resp.getAggregationResults().getAsMinAggregationResult("SomeName1").getValue());
    }

    /**
     * On the results of MatchQuery, calculate the minimum, maximum, average, sum, count, and distinct count for different fields.
     */
    public static void agg(SyncClient client) {
        SearchRequest searchRequest = SearchRequest.newBuilder()
            .tableName(TABLE_NAME)
            .indexName(INDEX_NAME)
            .returnAllColumns(true)
            .searchQuery(
                SearchQuery.newBuilder()
                    .query(QueryBuilders.match("fieldName1", "hello"))
                    .limit(10)
                    .getTotalCount(true)
                    .addAggregation(AggregationBuilders.min("SomeName1", "fieldName1"))
                    .addAggregation(AggregationBuilders.max("SomeName2", "fieldName1"))
                    .addAggregation(AggregationBuilders.sum("SomeName3", "fieldName1"))
                    .addAggregation(AggregationBuilders.avg("SomeName4", "fieldName2"))
                    .addAggregation(AggregationBuilders.count("SomeName5", "fieldName2"))
                    .addAggregation(AggregationBuilders.distinctCount("SomeName6", "fieldName2"))
                    .build())
            .build();
        SearchResponse resp = client.search(searchRequest);
        // Use the result
        AggregationResults results = resp.getAggregationResults();

        System.out.println(results.getAsMinAggregationResult("SomeName1").getValue());
        System.out.println(results.getAsMaxAggregationResult("SomeName2").getValue());
        System.out.println(results.getAsSumAggregationResult("SomeName3").getValue());
        System.out.println(results.getAsAvgAggregationResult("SomeName4").getValue());
        System.out.println(results.getAsCountAggregationResult("SomeName5").getValue());
        System.out.println(results.getAsDistinctCountAggregationResult("SomeName6").getValue());
    }

    /**
     * Perform grouping statistics on a field based on the results of MatchQuery.
     * Example: In inventory bills, there are "basketballs", "footballs", "badminton shuttles", etc. Conduct GroupByField aggregation on this field and return aggregation information such as "basketball: 10", "football: 5", "tennis: 1".
     */
    public static void groupByField(SyncClient client) {
        SearchRequest searchRequest = SearchRequest.newBuilder()
            .indexName(TABLE_NAME)
            .tableName(INDEX_NAME)
            .returnAllColumns(true)
            .searchQuery(
                SearchQuery.newBuilder()
                    .query(QueryBuilders.match("fieldName1", "hello"))
                    .limit(10)
                    .getTotalCount(true)
                    .addGroupBy(GroupByBuilders.groupByField("someName1", "someFieldName1"))
                    .build())
            .build();
        SearchResponse resp = client.search(searchRequest);
        // Use the result
        GroupByFieldResult results = resp.getGroupByResults().getAsGroupByFieldResult("someName1");
        // Iterate to fetch the results
        for (GroupByFieldResultItem item : results.getGroupByFieldResultItems()) {
            System.out.println("key:" + item.getKey());
            System.out.println("row count:" + item.getRowCount());
        }
    }

    /**
     * Perform grouping statistics on the results of MatchQuery based on the given range.
     */
    public static void groupByRange(SyncClient client) {
        SearchRequest searchRequest = SearchRequest.newBuilder()
            .indexName(TABLE_NAME)
            .tableName(INDEX_NAME)
            .returnAllColumns(true)
            .searchQuery(
                SearchQuery.newBuilder()
                    .query(QueryBuilders.match("fieldName1", "hello"))
                    .limit(10)
                    .getTotalCount(true)
                    .addGroupBy(GroupByBuilders
                        .groupByRange("someName1", "someFieldName1")
                        .addRange(Double.MIN_VALUE, 100)
                        .addRange(100, 500)
                        .addRange(500, Double.MAX_VALUE))
                    .build())
            .build();
        SearchResponse resp = client.search(searchRequest);
        // Use the result
        GroupByRangeResult results = resp.getGroupByResults().getAsGroupByRangeResult("someName1");
        // Iterate to fetch the results.
        for (GroupByRangeResultItem item : results.getGroupByRangeResultItems()) {
            System.out.println("row count:" + item.getRowCount());
        }
    }

    /**
     * Perform grouping statistics on the results of MatchQuery based on geographical latitude and longitude.
     */
    public static void groupByGeoDistance(SyncClient client) {
        SearchRequest searchRequest = SearchRequest.newBuilder()
            .tableName(INDEX_NAME)
            .indexName(TABLE_NAME)
            .returnAllColumns(true)
            .searchQuery(
                SearchQuery.newBuilder()
                    .query(QueryBuilders.match("fieldName1", "hello"))
                    .limit(10)
                    .getTotalCount(true)
                    .addGroupBy(GroupByBuilders
                        .groupByGeoDistance("someName1", "someFieldName1")
                        .origin(8.6545, 176.31231)   // Center coordinate point of latitude and longitude
                        .addRange(Double.MIN_VALUE, 100)
                        .addRange(100, 500)
                        .addRange(500, Double.MAX_VALUE))
                    .build())
            .build();
        SearchResponse resp = client.search(searchRequest);
        // Use the result
        GroupByRangeResult results = resp.getGroupByResults().getAsGroupByRangeResult("someName1");
        // Iterate to fetch the results.
        for (GroupByRangeResultItem item : results.getGroupByRangeResultItems()) {
            System.out.println("row count:" + item.getRowCount());
        }
    }

    /**
     * Perform grouping statistics on the results of MatchQuery based on the filter.
     */
    public static void groupByFilter(SyncClient client) {
        SearchRequest searchRequest = SearchRequest.newBuilder()
            .tableName(INDEX_NAME)
            .indexName(TABLE_NAME)
            .returnAllColumns(true)
            .searchQuery(
                SearchQuery.newBuilder()
                    .query(QueryBuilders.match("fieldName1", "hello"))
                    .limit(10)
                    .getTotalCount(true)
                    .addGroupBy(GroupByBuilders
                        .groupByFilter("someName1")
                        .addFilter(QueryBuilders.matchAll())
                        .addFilter(QueryBuilders.match("someFieldName2", "hi"))
                        .addFilter(QueryBuilders.range("someFieldName3").greaterThan(1000))
                        .addFilter(QueryBuilders.exists("someFieldName4"))
                    )
                    .build())
            .build();
        SearchResponse resp = client.search(searchRequest);
        // Use the result
        GroupByFilterResult results = resp.getGroupByResults().getAsGroupByFilterResult("someName1");
        // Iterate to fetch the results.
        for (GroupByFilterResultItem item : results.getGroupByFilterResultItems()) {
            System.out.println("row count:" + item.getRowCount());
        }
    }

    /**
     * Perform various combinations of agg and GroupBy on the results of MatchQuery, and support multi-layer nesting (subAgg and subGroupBy).
     */
    public static void groupByLotsOfGroupByAndAgg(SyncClient client) {
        SearchRequest searchRequest = SearchRequest.newBuilder()
            .tableName(INDEX_NAME)
            .indexName(TABLE_NAME)
            .returnAllColumns(true)
            .searchQuery(
                SearchQuery.newBuilder()
                    .query(QueryBuilders.match("fieldName1", "hello"))
                    .limit(10)
                    .getTotalCount(true)
                    .addAggregation(AggregationBuilders.min("SomeAggName1", "fieldName1"))
                    .addAggregation(AggregationBuilders.max("SomeAggName2", "fieldName1"))
                    .addGroupBy(GroupByBuilders
                        .groupByField("someName1", "someFieldName6")
                        .addSubAggregation(AggregationBuilders.max("subAgg1", "fieldName1"))
                        .addSubAggregation(AggregationBuilders.sum("subAgg2", "fieldName1")))
                    .addGroupBy(GroupByBuilders
                        .groupByRange("someName2", "someFieldName5")
                        .addRange(12, 90)
                        .addRange(100, 900))
                    .addGroupBy(GroupByBuilders
                        .groupByFilter("someName3")
                        .addFilter(QueryBuilders.matchAll())
                        .addFilter(QueryBuilders.match("someFieldName2", "hi"))
                        .addFilter(QueryBuilders.range("someFieldName3").greaterThan(1000))
                        .addFilter(QueryBuilders.exists("someFieldName4"))
                    )
                    .build())
            .build();
        SearchResponse resp = client.search(searchRequest);
        // The first layer of agg results
        AggregationResults aggResults = resp.getAggregationResults();
        System.out.println(aggResults.getAsMinAggregationResult("SomeAggName1").getValue());
        System.out.println(aggResults.getAsMaxAggregationResult("SomeAggName2").getValue());

        // Retrieve the first-level groupByField results and simultaneously fetch the nested agg results.
        GroupByFieldResult results = resp.getGroupByResults().getAsGroupByFieldResult("someName1");
        for (GroupByFieldResultItem item : results.getGroupByFieldResultItems()) {
            System.out.println("row count:" + item.getRowCount());
            System.out.println("key:" + item.getKey());
            System.out.println(item.getSubAggregationResults().getAsMaxAggregationResult("subAgg1"));
            System.out.println(item.getSubAggregationResults().getAsSumAggregationResult("subAgg2"));
        }

        // Extract the results of the first layer of GroupByRange and GroupByFilter without nesting.
        //GroupByFilter
        GroupByFilterResult results1 = resp.getGroupByResults().getAsGroupByFilterResult("someName3");
        for (GroupByFilterResultItem item : results1.getGroupByFilterResultItems()) {
            System.out.println("row count:" + item.getRowCount());
        }

        //GroupByRange
        GroupByRangeResult results2 = resp.getGroupByResults().getAsGroupByRangeResult("someName2");
        for (GroupByRangeResultItem item : results2.getGroupByRangeResultItems()) {
            System.out.println("row count: " + item.getRowCount());
            System.out.println("from: " + item.getFrom());
            System.out.println("to: " + item.getTo());
        }
    }


    /**
     * Get data chunk information.
     * Purpose: Used to set the maximum parallelism in {@link ScanQuery} {@link ScanQuery#setMaxParallel(Integer)} and sessionId {@link ParallelScanRequest#setSessionId(byte[])}
     */
    public static ComputeSplitsResponse computeSplits(SyncClient client, String tableName, String indexName){
        ComputeSplitsRequest computeSplitsRequest = new ComputeSplitsRequest();
        computeSplitsRequest.setTableName(tableName);
        computeSplitsRequest.setSplitsOptions(new SearchIndexSplitsOptions(indexName));
        return client.computeSplits(computeSplitsRequest);
    }



    /**
     * ScanQuery retrieves data, native interface usage example. It is recommended to use {@link SyncClientInterface#createParallelScanIterator(ParallelScanRequest)}.
     */
    public static void scanQuery(SyncClient client, String tableName, String indexName){
        // Calculate the number of splits and create a session
        ComputeSplitsResponse computeSplitsResponse = computeSplits(client, tableName, indexName);
        // Get data based on the session created in the previous step.
        ParallelScanRequest parallelScanRequest = ParallelScanRequest.newBuilder()
            .tableName(tableName)
            .indexName(indexName)
            .scanQuery(ScanQuery.newBuilder()
                .query(QueryBuilders.range("col_long").lessThan(123)) // The query here determines what data to retrieve.
                .limit(100)
                .build())
            .addColumnsToGet("col_bool", "col_keyword", "col_long")  // Must be a field in the index
            .sessionId(computeSplitsResponse.getSessionId())
            .build();

        ParallelScanResponse response = client.parallelScan(parallelScanRequest);

        int total = 0;
        // Continuously fetch and consume data
        while (null != response.getNextToken()) {
            // Get the data and consume it
            List<Row> rows = response.getRows();
            total += rows.size();

            // Initialization for the next request
            parallelScanRequest.getScanQuery().setToken(response.getNextToken());
            response = client.parallelScan(parallelScanRequest);
        }
        System.out.println("use native parallelScan interface, row count: " + total);
    }


    /**
     * Get data with ScanQuery through {@link SyncClientInterface#createParallelScanIterator(ParallelScanRequest)}.
     */
    public static void scanQueryByRowIterator(SyncClient client, String tableName, String indexName){
        // Calculate the number of splits and create a session
        ComputeSplitsResponse computeSplitsResponse = computeSplits(client, tableName, indexName);
        // Get data based on the session created in the previous step.
        ParallelScanRequest parallelScanRequest = ParallelScanRequest.newBuilder()
            .tableName(tableName)
            .indexName(indexName)
            .scanQuery(ScanQuery.newBuilder()
                .query(QueryBuilders.range("col_long").lessThan(123)) // The query here determines what data to retrieve.
                .limit(100)
                .build())
            .addColumnsToGet("col_bool", "col_keyword", "col_long")  // Must be a field in the index
            .sessionId(computeSplitsResponse.getSessionId())
            .build();

        RowIterator ltr = client.createParallelScanIterator(parallelScanRequest);

        int count = 0;
        while (ltr.hasNext()) {
            Row next = ltr.next();
            count++;
        }
        System.out.println("ParallelScan row count:" + count);
    }

    /**
     * Perform histogram statistics based on the given fieldRange.
     */
    public static void groupByHistogram(SyncClient client) {
        SearchRequest searchRequest = SearchRequest.newBuilder()
            .indexName(INDEX_NAME)
            .tableName(TABLE_NAME)
            .returnAllColumns(true)
            .searchQuery(
                SearchQuery.newBuilder()
                    .addGroupBy(GroupByBuilders
                        .groupByHistogram("someName1", "Col_Double")
                        .interval(2.0)
                        .offset(1.0) // The starting point of each group will be 1.0, 3.0, 5.0..., rather than 0.0, 2.0, 4.0...
                        .addFieldRange(1.0, 5.0))
                    .build())
            .build();
        SearchResponse resp = client.search(searchRequest);
        // Use the result
        GroupByHistogramResult results = resp.getGroupByResults().getAsGroupByHistogramResult("someName1");
        // Iterate to fetch the results
        for (GroupByHistogramItem item : results.getGroupByHistogramItems()) {
            System.out.println("key: " + item.getKey().asDouble() + " value:" + item.getValue());
        }
    }

    /**
     * Composite type grouping aggregation: performs grouping aggregation based on multiple source groupBy inputs (supported types: groupbyField, groupByHistogram, groupByDataHistogram).
     * The results of multi-column aggregations are returned in a flattened structure.
     */
    public static void groupByComposite(SyncClient client) {
        GroupByComposite.Builder compositeBuilder = GroupByBuilders
                .groupByComposite("groupByComposite")
                .addSources(GroupByBuilders.groupByField("groupByField", "Col_Keyword").build())
                .addSources(GroupByBuilders.groupByHistogram("groupByHistogram", "Col_Long").interval(5).build())
                .addSources(GroupByBuilders.groupByDateHistogram("groupByDateHistogram", "Col_Date").interval(5, DateTimeUnit.DAY).timeZone("+05:30").build());

        SearchRequest searchRequest = SearchRequest.newBuilder()
                .indexName(INDEX_NAME)
                .tableName(TABLE_NAME)
                .returnAllColumnsFromIndex(true)
                .searchQuery(SearchQuery.newBuilder()
                        .addGroupBy(compositeBuilder.build())
                        .build())
                .build();

        SearchResponse resp = client.search(searchRequest);

        while (true) {
            if (resp.getGroupByResults() == null || resp.getGroupByResults().getResultAsMap().size() == 0) {
                System.out.println("groupByComposite Result is null or empty");
                return;
            }

            GroupByCompositeResult result = resp.getGroupByResults().getAsGroupByCompositeResult("groupByComposite");

            if(result.getSourceNames().size() != 0) {
                for (String sourceGroupByNames: result.getSourceNames()) {
                    System.out.printf("%s\t", sourceGroupByNames);
                }
                System.out.print("rowCount\t\n");
            }


            for (GroupByCompositeResultItem item : result.getGroupByCompositeResultItems()) {
                for (String value : item.getKeys()) {
                    System.out.printf("%s\t", value);
                }
                System.out.printf("%d\t\n", item.getRowCount());
            }

            if (result.getNextToken() != null) {
                searchRequest.setSearchQuery(
                        SearchQuery.newBuilder()
                                .addGroupBy(compositeBuilder.nextToken(result.getNextToken()).build())
                                .build()
                );
                resp = client.search(searchRequest);
            } else {
                break;
            }
        }
    }

    /**
     * Date histogram aggregation: Assuming the table stores order data, the following code implements counting the number of orders sold per day, and by adding subAgg, it also calculates the maximum selling price for each day.
     */
    public static void groupByDateHistogram(SyncClient client) {
        // Construct the query statement.
        SearchRequest searchRequest = SearchRequest.newBuilder()
                .returnAllColumns(false)
                .tableName(TABLE_NAME)
                .indexName(INDEX_NAME)
                .searchQuery(
                        SearchQuery.newBuilder()
                                .query(QueryBuilders.matchAll())
                                .limit(0)
                                .getTotalCount(false)
                                .addGroupBy(GroupByBuilders
                                        .groupByDateHistogram("groupByDateHistogram", "Col_Date")
                                        .interval(1, DateTimeUnit.DAY)  // One group per day
                                        .offset(6, DateTimeUnit.HOUR) // The starting point of each group is 6:00 on the day (set the deviation to 6 hours).
                                        .minDocCount(1)     // The group will be returned only when the count within the group is greater than 1.
                                        .timeZone("+05:30")  // If the 'Col_Date' field does not contain time zone information, you can specify the timeZone. When performing statistical grouping, this will determine which day's group the time falls into. For example, for the Indian time zone, you can enter: +05:30
                                        .missing("2017-05-10 12:00:00") // If the 'Col_Date' field of a row of data is empty, use this value for statistics.
                                        .fieldRange("2017-05-01 00:00", "2017-05-21 00:00:00")  // Only count from May 1st to May 21st.
                                        .addSubAggregation(AggregationBuilders.max("subAggName", "Column_Price")) // Add sub-statistical aggregation to find the maximum price within each group.
                                )
                                .build())
                .build();
        // Execute the query.
        SearchResponse resp = client.search(searchRequest);
        // Get the statistical aggregation results of the date histogram.
        List<GroupByDateHistogramItem> items = resp.getGroupByResults().getAsGroupByDateHistogramResult("groupByDateHistogram").getGroupByDateHistogramItems();
        for (GroupByDateHistogramItem item : items) {
            // Get the maximum price within the group
            double maxPrice = item.getSubAggregationResults().getAsMaxAggregationResult("subAggName").getValue();
            System.out.printf("millisecondTimestamp:%d, count:%d, maxPrice:%s \n", item.getTimestamp(), item.getRowCount(), maxPrice);
        }
    }

    /**
     * Geographical coordinate statistics: Assuming the table stores a series of geographical coordinates, the following code can be used to count the number of geographical coordinates within each geographical area (divided according to GeoHash).
     */
    public static void groupByGeoGrid(SyncClient client) {
        // Construct the query statement.
        SearchRequest searchRequest = SearchRequest.newBuilder()
                .returnAllColumns(false)
                .tableName(TABLE_NAME)
                .indexName(INDEX_NAME)
                .searchQuery(
                        SearchQuery.newBuilder()
                                .query(QueryBuilders.matchAll())
                                .limit(0)
                                .getTotalCount(false)
                                .addGroupBy(GroupByBuilders
                                        .groupByGeoGrid("groupByGeoGrid", "Col_Geo_Point")
                                        .size(100)  // Set the number of returned buckets to 100.
                                        .precision(GeoHashPrecision.GHP_152M_152M_7) // Set the grouping granularity to GHP_152M_152M
                                )
                                .build())
                .build();
        // Execute the query.
        SearchResponse resp = client.search(searchRequest);
        // Get the statistical aggregation results of geographical locations.
        List<GroupByGeoGridResultItem> items = resp.getGroupByResults().getAsGroupByGeoGridResult("groupByGeoGridResult").getGroupByGeoGridResultItems();
        for (GroupByGeoGridResultItem item : items) {
            // Get the key, geoGrid, and rowCount within the group.
            String geoHash = item.getKey();
            GeoGrid geoGrid = item.getGeoGrid();
            Long rowCount = item.getRowCount();
        }
    }
    /**
     * Perform percentage statistics on the results of MatchQuery.
     */
    public static void percentilesAggDouble(SyncClient client) {

        SearchRequest searchRequest = SearchRequest.newBuilder()
            .tableName(TABLE_NAME)
            .indexName(INDEX_NAME)
            .returnAllColumns(true)
            .searchQuery(
                SearchQuery.newBuilder()
                    .addAggregation(AggregationBuilders.percentiles("percentilesAgg", "Col_Double")
                        .percentiles(new ArrayList<Double>() {
                            {
                                this.add(1.0);
                                this.add(50.0);
                                this.add(99.0);
                            }
                        })
                        .missing(1.0))
                    .build())
            .build();
        SearchResponse resp = client.search(searchRequest);

        // Use the result
        PercentilesAggregationResult percentilesAggregationResult = resp.getAggregationResults().getAsPercentilesAggregationResult(
            "percentilesAgg");
        for (PercentilesAggregationItem item : percentilesAggregationResult.getPercentilesAggregationItems()) {
            System.out.println("key: " + item.getKey() + " value:" + item.getValue().asDouble());
        }
    }

    /**
     * Perform percentage statistics on the results of MatchQuery.
     */
    public static void percentilesAggLong(SyncClient client) {

        SearchRequest searchRequest = SearchRequest.newBuilder()
            .tableName(TABLE_NAME)
            .indexName(INDEX_NAME)
            .returnAllColumns(true)
            .searchQuery(
                SearchQuery.newBuilder()
                    .addAggregation(AggregationBuilders.percentiles("percentilesAgg", "Col_Long")
                        .percentiles(new ArrayList<Double>() {
                            {
                                this.add(1.0);
                                this.add(50.0);
                                this.add(99.0);
                            }
                        })
                        .missing(1L))
                    .build())
            .build();
        SearchResponse resp = client.search(searchRequest);

        // Use the result
        PercentilesAggregationResult percentilesAggregationResult = resp.getAggregationResults().getAsPercentilesAggregationResult(
            "percentilesAgg");
        for (PercentilesAggregationItem item : percentilesAggregationResult.getPercentilesAggregationItems()) {
            System.out.println("key: " + item.getKey() + " value:" + item.getValue().asLong());
        }
    }

    /**
     * MatchQuery keyword highlighting
     */
    public static void matchQueryWithHighlighting(SyncClient client) {
        SearchRequest searchRequest = SearchRequest.newBuilder()
            .tableName(TABLE_NAME)
            .indexName(INDEX_NAME)
            .returnAllColumnsFromIndex(true)
            .searchQuery(SearchQuery.newBuilder()
                .limit(5)
                .query(QueryBuilders.bool()
                    .should(QueryBuilders.match("Col_Text", "hangzhou shanghai"))
                    .should(QueryBuilders.nested()
                        .path("Col_Nested")
                        .scoreMode(ScoreMode.Min)
                        .query(QueryBuilders.bool()
                            .should(QueryBuilders.match("Col_Nested.Level1_Col1_Text", "hangzhou shanghai"))
                            .should(QueryBuilders.nested()
                                .path("Col_Nested.Level1_Col2_Nested")
                                .scoreMode(ScoreMode.Min)
                                .query(QueryBuilders.match("Col_Nested.Level1_Col2_Nested.Level2_Col1_Text", "hangzhou shanghai"))
                                .innerHits(InnerHits.newBuilder()
                                    .highlight(Highlight.newBuilder()
                                        .addFieldHighlightParam("Col_Nested.Level1_Col2_Nested.Level2_Col1_Text", HighlightParameter.newBuilder().build())
                                        .build())
                                    .build())))
                        .innerHits(InnerHits.newBuilder()
                            .sort(new Sort(Arrays.asList(
                                new ScoreSort(),
                                new DocSort()
                            )))
                            .highlight(Highlight.newBuilder()
                                .addFieldHighlightParam("Col_Nested.Level1_Col1_Text", HighlightParameter.newBuilder().build())
                                .build())
                            .build())))
                .highlight(Highlight.newBuilder()
                    .addFieldHighlightParam("Col_Text", HighlightParameter.newBuilder()
                        .highlightFragmentOrder(HighlightFragmentOrder.TEXT_SEQUENCE)
                        .preTag("<b>")
                        .postTag("</b>")
                        .build())
                    .build())
                .build())
            .build();
        SearchResponse resp = client.search(searchRequest);

        printSearchHit(resp.getSearchHits(), "");
    }

    /**
     * ScanQuery retrieves data and retries upon encountering an exception.
     */
    public static void scanQueryByRowIteratorWithException(SyncClient client, String tableName, String indexName){
        try {
            scanQueryByRowIterator(client, tableName, indexName);
        } catch (TableStoreException ex) {
            if (ex.getErrorCode().equals("OTSSessionExpired")){
                // Session is invalid, retry
                scanQueryByRowIterator(client, tableName, indexName);
            }else {
                ex.printStackTrace();
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    /**
     * ScanQuery retrieves data using multi-threaded parallel data fetching.
     */
    public static void scanQueryByMultiThread(SyncClient client) throws InterruptedException {
        // Calculate the split count and create session
        ComputeSplitsResponse computeSplitsResponse = computeSplits(client, "tableName", "indexName");

        final class ThreadForScanQuery extends Thread {
            private SyncClient client;
            private ParallelScanRequest parallelScanRequest;

            private ThreadForScanQuery(SyncClient client, int currentParallelId, int maxParallel, byte[] sessionId) {
                this.client = client;
                this.setName(maxParallel + "-" + currentParallelId);  // Set the thread name
                this.parallelScanRequest = ParallelScanRequest.newBuilder()
                    .tableName("tableName")
                    .indexName("indexName")
                    .scanQuery(ScanQuery.newBuilder()
                        .query(QueryBuilders.range("col_long").lessThan(123)) // The query here determines what data to retrieve.
                        .limit(100)  // How much data to retrieve for each network request
                        .currentParallelId(currentParallelId)
                        .maxParallel(maxParallel)
                        .build())
                    .addColumnsToGet("col_bool", "col_keyword", "col_long")  // Must be a field in the index
                    .sessionId(sessionId)
                    .build();
            }
            @Override
            public void run() {
                RowIterator ltr = client.createParallelScanIterator(parallelScanRequest);
                System.out.println("thread name:" + this.getName());

                // Consume data
                int count = 0;
                while (ltr.hasNext()) {
                    Row next = ltr.next(); // Add your own processing logic
                    count++;
                }
                System.out.println("thread name:" + this.getName() + ", row count:" + count);
            }
        }

        // Maximum number of threads, it's not recommended to exceed maxParallel, otherwise the performance may drop significantly.
        int maxParallel = computeSplitsResponse.getSplitsSize();
        byte[] sessionId = computeSplitsResponse.getSessionId();

        // Multiple threads run concurrently, the value range of currentParallelId is [0, maxParallel)
        List<ThreadForScanQuery> threadList = new ArrayList<ThreadForScanQuery>();
        for (int i = 0; i < maxParallel; i++) {
            ThreadForScanQuery thread = new ThreadForScanQuery(client, i, maxParallel, sessionId);
            threadList.add(thread);
        }

        // Start
        for (ThreadForScanQuery thread : threadList) {
            thread.start();
        }

        // Blocking wait
        for (ThreadForScanQuery thread : threadList) {
            thread.join();
        }
        System.out.println("all thread done!");
    }

    /**
     * Print the content of searchHit
     * @param searchHits
     * @param prefix
     */
    private static void printSearchHit(List<SearchHit> searchHits, String prefix) {
        for (SearchHit searchHit : searchHits) {
            if (searchHit.getScore() != null) {
                System.out.printf("%s Score: %s\n", prefix, searchHit.getScore());
            }

            if (searchHit.getOffset() != null) {
                System.out.printf("%s Offset: %s\n", prefix, searchHit.getOffset());
            }

            if (searchHit.getRow() != null) {
                System.out.printf("%s Row: %s\n", prefix, searchHit.getRow().toString());
            }

            if (searchHit.getHighlightResultItem() != null) {
                System.out.printf("%s Highlight: \n", prefix);
                StringBuilder strBuilder = new StringBuilder();
                for (Map.Entry<String, HighlightField> entry : searchHit.getHighlightResultItem().getHighlightFields().entrySet()) {
                    strBuilder.append(entry.getKey()).append(":").append("[");
                    strBuilder.append(StringUtils.join(",", entry.getValue().getFragments())).append("]\n");
                }
                System.out.printf("%s   %s", prefix, strBuilder);
            }

            for (SearchInnerHit searchInnerHit : searchHit.getSearchInnerHits().values()) {
                System.out.printf("%s Path: %s\n", prefix, searchInnerHit.getPath());
                System.out.printf("%s InnerHit: \n", prefix);
                printSearchHit(searchInnerHit.getSubSearchHits(), prefix + "    ");
            }

            System.out.println();
        }
    }
}
