package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
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
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFieldResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFieldResultItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFilterResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFilterResultItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByHistogramItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByHistogramResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByRangeResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByRangeResultItem;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SearchIndexSample {

    /**
     * 本示例中建立一张表,名为search_index_sample_table,两个主键, 主键分别为pk1，pk2.
     * 给这张表建立一个SearchIndex，然后列出表下的SearchIndex，然后查询SearchIndex的信息。
     * 向表内写入几条数据，并通过几种search query进行查询。
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
            // 建表
            createTable(client);

            System.out.println("create table succeeded.");

            // 创建一个SearchIndex
            createSearchIndex(client);
            System.out.println("create search index succeeded.");

            // 列出表下的所有SearchIndex
            System.out.println(System.currentTimeMillis());
            List<SearchIndexInfo> indexInfos = listSearchIndex(client);
            System.out.println("list search index succeeded, indexInfo: \n" + indexInfos);
            System.out.println(System.currentTimeMillis());

            // 查询SearchIndex的描述信息
            DescribeSearchIndexResponse describeSearchIndexResponse = describeSearchIndex(client);
            System.out.println("describe search index succeeded, response: \n" + describeSearchIndexResponse.jsonize());

            // 等待表load完毕, searchIndex初始化完成.
            try {
                System.out.println("sleeping...");
                Thread.sleep(20 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // putRow写入几行数据
            putRow(client);
            System.out.println("put row succeeded.");

            // 等待数据同步到SearchIndex
            waitUntilAllDataSync(client, INDEX_NAME, 6);

            // 使用matchAllQuery查询数据总行数
            System.out.println("MatchAllQuery...");
            matchAllQuery(client);

            // 使用MatchQuery查询数据
            System.out.println("MatchQuery...");
            matchQuery(client);

            // 使用RangeQuery查询数据，并排序
            System.out.println("RangeQuery...");
            rangeQuery(client);

            // 使用MatchPhraseQuery查询数据
            System.out.println("MatchPhraseQuery...");
            matchPhraseQuery(client);

            // 使用PrefixQuery查询数据
            System.out.println("PrefixQuery...");
            prefixQuery(client);

            // 使用WildcardQuery查询数据
            System.out.println("WildcardQuery...");
            wildcardQuery(client);

            // 使用TermQuery查询数据
            System.out.println("TermQuery...");
            termQuery(client);

            // 使用BoolQuery查询数据
            System.out.println("BoolQuery...");
            boolQuery(client);

            // 使用ExistsQuery查询数据
            System.out.println("ExistsQuery...");
            existsQuery(client);

            // 使用groupByHistogram查询数据
            System.out.println("groupByHistogram...");
            groupByHistogram( client);

            // 使用percentilesAggLong查询数据
            System.out.println("percentilesAggLong(client);...");
            percentilesAggLong(client);


            // 动态修改schema
            System.out.println("DynamicModifySchema...");
            dynamicModifySchema(client);

        } catch (TableStoreException e) {
            System.err.println("操作失败，详情：" + e.getMessage());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("请求失败，详情：" + e.getMessage());
        } finally {
            //             // 为了安全，这里不能默认删除索引和表，如果需要删除，需用户自己手动打开
            //            try {
            //                 // 删除SearchIndex
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

        int timeToLive = -1; // 数据的过期时间, 单位秒, -1代表永不过期. 假如设置过期时间为一年, 即为 365 * 24 * 3600.
        int maxVersions = 1; // 保存的最大版本数, 设置为1即代表每列上最多保存一个版本(保存最新的版本).

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
            new FieldSchema("Col_Long", FieldType.LONG).setIndex(true).setEnableSortAndAgg(true),
            new FieldSchema("Col_Text", FieldType.TEXT).setIndex(true)));
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
                .setAnalyzerParameter(analyzerParameter)));
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
            new FieldSchema("Col_Text", FieldType.TEXT).setIndex(true),
            new FieldSchema("Timestamp", FieldType.LONG).setIndex(true).setEnableSortAndAgg(true)));
        indexSchema.setIndexSort(new Sort(
            Arrays.<Sort.Sorter>asList(new FieldSort("Timestamp", SortOrder.ASC))));
        request.setIndexSchema(indexSchema);
        client.createSearchIndex(request);
    }

    /**
     * 使用Token进行翻页。
     **/
    private static void readMoreRowsWithToken(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(new MatchAllQuery());
        searchQuery.setGetTotalCount(true); // 需要设置GetTotalCount为true才会返回满足条件的数据总行数
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
        long[] longValues = {1, 2, 3, 4, 5, 6};
        for (int i = 0; i < 5; i++) {
            // 构造主键
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("sample"));
            primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(i));
            PrimaryKey primaryKey = primaryKeyBuilder.build();

            RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);

            //加入一些属性列
            rowPutChange.addColumn("Col_Keyword", ColumnValue.fromString(keywords[i]));
            rowPutChange.addColumn("Col_Long", ColumnValue.fromLong(longValues[i]));

            rowPutChange.addColumn("Col_Text", ColumnValue.fromString(keywords[i]));
            rowPutChange.addColumn("Col_Boolean", ColumnValue.fromBoolean(i % 2 == 0 ? true : false));
            client.putRow(new PutRowRequest(rowPutChange));
        }
        {   // 构造一行缺失 Col_Keyword 和 Col_Text列
            // 构造主键
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("sample"));
            primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(5));
            PrimaryKey primaryKey = primaryKeyBuilder.build();

            RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);

            //加入一些属性列
            rowPutChange.addColumn("Col_Long", ColumnValue.fromLong(longValues[5]));
            rowPutChange.addColumn("Col_Boolean", ColumnValue.fromBoolean(false));
            client.putRow(new PutRowRequest(rowPutChange));
        }
    }

    private static void waitUntilAllDataSync(SyncClient client, String indexName, long expectTotalHit) {
        long begin = System.currentTimeMillis();
        while (true) {
            SearchQuery searchQuery = new SearchQuery();
            searchQuery.setQuery(new MatchAllQuery());
            searchQuery.setGetTotalCount(true);
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
     * 通过MatchAllQuery查询表中数据的总行数
     */
    private static void matchAllQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(new MatchAllQuery());
        /**
         * MatchAllQuery结果中的TotalCount可以表示表中数据的总行数，
         * 如果只为了取TotalCount，可以设置limit=0，即不返回任意一行数据。
         */
        searchQuery.setLimit(0);
        searchQuery.setGetTotalCount(true); // 需要设置GetTotalCount为true才会返回满足条件的数据总行数
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);
        SearchResponse resp = client.search(searchRequest);
        /**
         * 判断返回的结果是否是完整的，当isAllSuccess为false时，代表可能有部分节点查询失败，返回的是部分数据
         */
        if (!resp.isAllSuccess()) {
            System.out.println("NotAllSuccess!");
        }
        System.out.println("IsAllSuccess: " + resp.isAllSuccess());
        System.out.println("TotalCount: " + resp.getTotalCount());
        System.out.println(resp.getRequestId());
    }

    /**
     * 查询表中Col_Keyword这一列的值能够匹配"hangzhou"的数据，返回匹配到的总行数和一些匹配成功的行。
     */
    private static void matchQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        MatchQuery matchQuery = new MatchQuery(); // 设置查询类型为MatchQuery
        matchQuery.setFieldName("Col_Keyword"); // 设置要匹配的字段
        matchQuery.setText("hangzhou"); // 设置要匹配的值
        searchQuery.setQuery(matchQuery);
        searchQuery.setOffset(0); // 设置offset为0
        searchQuery.setLimit(20); // 设置limit为20，表示最多返回20行数据
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);
        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount());
        System.out.println("Row: " + resp.getRows()); // 不设置columnsToGet，默认只返回主键

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // 设置返回所有列
        searchRequest.setColumnsToGet(columnsToGet);

        resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 查询表中Col_Text这一列的值能够匹配"hangzhou shanghai"的数据，匹配条件为短语匹配(要求短语完整的按照顺序匹配)，返回匹配到的总行数和一些匹配成功的行。
     */
    private static void matchPhraseQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery(); // 设置查询类型为MatchPhraseQuery
        matchPhraseQuery.setFieldName("Col_Text"); // 设置要匹配的字段
        matchPhraseQuery.setText("hangzhou shanghai"); // 设置要匹配的值
        searchQuery.setQuery(matchPhraseQuery);
        searchQuery.setOffset(0); // 设置offset为0
        searchQuery.setLimit(20); // 设置limit为20，表示最多返回20行数据
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);
        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount());
        System.out.println("Row: " + resp.getRows()); // 默认只返回主键

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // 设置返回所有列
        searchRequest.setColumnsToGet(columnsToGet);

        resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 查询表中Col_Keyword这一列精确匹配"hangzhou"的数据。
     */
    private static void termQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        TermQuery termQuery = new TermQuery(); // 设置查询类型为TermQuery
        termQuery.setFieldName("Col_Keyword"); // 设置要匹配的字段
        termQuery.setTerm(ColumnValue.fromString("hangzhou")); // 设置要匹配的值
        searchQuery.setQuery(termQuery);
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // 设置返回所有列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 查询表中Col_Keyword这一列精确匹配"hangzhou"或"shanghai"的数据。
     * TermsQuery可以使用多个Term同时查询。
     */
    private static void termsQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        TermsQuery termsQuery = new TermsQuery(); // 设置查询类型为TermQuery
        termsQuery.setFieldName("Col_Keyword"); // 设置要匹配的字段
        termsQuery.setTerms(Arrays.asList(ColumnValue.fromString("hangzhou"),
            ColumnValue.fromString("shanghai"))); // 设置要匹配的值
        searchQuery.setQuery(termsQuery);
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // 设置返回所有列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 查询表中Col_Keyword这一列前缀为"hangzhou"的数据。
     */
    private static void prefixQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        PrefixQuery prefixQuery = new PrefixQuery(); // 设置查询类型为PrefixQuery
        prefixQuery.setFieldName("Col_Keyword");
        prefixQuery.setPrefix("hangzhou");
        searchQuery.setQuery(prefixQuery);
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // 设置返回所有列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 使用通配符查询，查询表中Col_Keyword这一列的值匹配"hang*u"的数据
     */
    private static void wildcardQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        WildcardQuery wildcardQuery = new WildcardQuery(); // 设置查询类型为WildcardQuery
        wildcardQuery.setFieldName("Col_Keyword");
        wildcardQuery.setValue("hang*u"); //wildcardQuery支持通配符
        searchQuery.setQuery(wildcardQuery);
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // 设置返回所有列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);

        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 查询表中Col_Long这一列大于3的数据，结果按照Col_Long这一列的值逆序排序。
     */
    private static void rangeQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        RangeQuery rangeQuery = new RangeQuery(); // 设置查询类型为RangeQuery
        rangeQuery.setFieldName("Col_Long");  // 设置针对哪个字段
        rangeQuery.greaterThan(ColumnValue.fromLong(3));  // 设置该字段的范围条件，大于3
        searchQuery.setQuery(rangeQuery);
        searchQuery.setGetTotalCount(true);
        // 设置按照Col_Long这一列逆序排序
        FieldSort fieldSort = new FieldSort("Col_Long");
        fieldSort.setOrder(SortOrder.DESC);
        searchQuery.setSort(new Sort(Arrays.asList((Sort.Sorter)fieldSort)));

        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * Col_GeoPoint是GeoPoint类型，查询表中Col_GeoPoint这一列的值在左上角为"10,0", 右下角为"0,10"的矩形范围内的数据。
     */
    public static void geoBoundingBoxQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        GeoBoundingBoxQuery geoBoundingBoxQuery = new GeoBoundingBoxQuery(); // 设置查询类型为GeoBoundingBoxQuery
        geoBoundingBoxQuery.setFieldName("Col_GeoPoint"); // 设置比较哪个字段的值
        geoBoundingBoxQuery.setTopLeft("10,0"); // 设置矩形左上角
        geoBoundingBoxQuery.setBottomRight("0,10"); // 设置矩形右下角
        searchQuery.setQuery(geoBoundingBoxQuery);
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setColumns(Arrays.asList("Col_GeoPoint"));  //设置返回Col_GeoPoint这一列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 查询表中Col_GeoPoint这一列的值距离中心点不超过一定距离的数据。
     */
    public static void geoDistanceQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        GeoDistanceQuery geoDistanceQuery = new GeoDistanceQuery();  // 设置查询类型为GeoDistanceQuery
        geoDistanceQuery.setFieldName("Col_GeoPoint");
        geoDistanceQuery.setCenterPoint("5,5"); // 设置中心点
        geoDistanceQuery.setDistanceInMeter(10000); // 设置到中心点的距离条件，不超过10000米
        searchQuery.setQuery(geoDistanceQuery);
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setColumns(Arrays.asList("Col_GeoPoint"));  //设置返回Col_GeoPoint这一列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 查询表中Col_GeoPoint这一列的值在一个给定多边形范围内的数据。
     */
    public static void geoPolygonQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        GeoPolygonQuery geoPolygonQuery = new GeoPolygonQuery();  // 设置查询类型为GeoPolygonQuery
        geoPolygonQuery.setFieldName("Col_GeoPoint");
        geoPolygonQuery.setPoints(Arrays.asList("0,0", "5,5", "5,0")); // 设置多边形的顶点
        searchQuery.setQuery(geoPolygonQuery);
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setColumns(Arrays.asList("Col_GeoPoint"));  //设置返回Col_GeoPoint这一列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 通过BoolQuery进行复合条件查询。
     */
    public static void boolQuery(SyncClient client) {
        /*
         * 查询条件一：RangeQuery，Col_Long这一列的值要大于3
         */
        RangeQuery rangeQuery = new RangeQuery();
        rangeQuery.setFieldName("Col_Long");
        rangeQuery.greaterThan(ColumnValue.fromLong(3));

        /*
         * 查询条件二：MatchQuery，Col_Keyword这一列的值要匹配"hangzhou"
         */
        MatchQuery matchQuery = new MatchQuery(); // 设置查询类型为MatchQuery
        matchQuery.setFieldName("Col_Keyword"); // 设置要匹配的字段
        matchQuery.setText("hangzhou"); // 设置要匹配的值

        SearchQuery searchQuery = new SearchQuery();
        {
            /*
             * 构造一个BoolQuery，设置查询条件是必须同时满足"条件一"和"条件二"
             */
            BoolQuery boolQuery = new BoolQuery();
            boolQuery.setMustQueries(Arrays.asList(rangeQuery, matchQuery));
            searchQuery.setQuery(boolQuery);
            searchQuery.setGetTotalCount(true);
            SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);
            SearchResponse resp = client.search(searchRequest);
            System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
            System.out.println("Row: " + resp.getRows());
        }

        {
            /*
             * 构造一个BoolQuery，设置查询条件是至少满足"条件一"和"条件二"中的一个条件
             */
            BoolQuery boolQuery = new BoolQuery();
            boolQuery.setShouldQueries(Arrays.asList(rangeQuery, matchQuery));
            boolQuery.setMinimumShouldMatch(1); // 设置最少满足一个条件
            searchQuery.setQuery(boolQuery);
            searchQuery.setGetTotalCount(true);
            SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);
            SearchResponse resp = client.search(searchRequest);
            System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
            System.out.println("Row: " + resp.getRows());
        }
    }

    public static void existsQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        ExistsQuery existsQuery = new ExistsQuery();
        existsQuery.setFieldName("Col_Keyword");
        searchQuery.setQuery(existsQuery);
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true); // 设置返回所有列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    public static void dynamicModifySchema(SyncClient client) {
        //step 1. 创建"重建索引"
        //若需要删除源索引的索引列Col_Long，并新增一列Col_Boolean，则可以重建索引，如下：
        CreateSearchIndexRequest request = new CreateSearchIndexRequest();
        request.setTableName(TABLE_NAME);
        request.setIndexName(INDEX_NAME_SCHEMA_MODIFIED); //重建索引：修改schema后的索引
        request.setSourceIndexName(INDEX_NAME); //源索引：被修改schema的索引

        IndexSchema indexSchema = new IndexSchema();
        indexSchema.setFieldSchemas(Arrays.asList(
                new FieldSchema("Col_Keyword", FieldType.KEYWORD).setIndex(true).setEnableSortAndAgg(true),
                new FieldSchema("Col_Long", FieldType.LONG).setIndex(true).setEnableSortAndAgg(true),
                //new FieldSchema("Col_Text", FieldType.TEXT).setIndex(true),         //删除索引列
                new FieldSchema("Col_Boolean", FieldType.BOOLEAN).setIndex(true)  //新增索引列
        ));
        request.setIndexSchema(indexSchema);
        client.createSearchIndex(request);

        //step 2. 等待"重建索引"数据同步。先后经历"全量同步"和"增量同步"两个阶段
        waitUntilAllDataSync(client, INDEX_NAME, 6);

        //step 3. AB test
        {   //设置查询流量的权重：源索引80%, 重建索引80%
            UpdateSearchIndexRequest updateSearchIndexRequest = new UpdateSearchIndexRequest(TABLE_NAME, INDEX_NAME,
                    Arrays.asList(
                            new QueryFlowWeight(INDEX_NAME, 80),
                            new QueryFlowWeight(INDEX_NAME_SCHEMA_MODIFIED, 20)
                    ));
            client.updateSearchIndex(updateSearchIndexRequest);

            //检查权重设置是否成功
            DescribeSearchIndexRequest describeSearchIndexRequest = new DescribeSearchIndexRequest();
            describeSearchIndexRequest.setTableName(TABLE_NAME);
            describeSearchIndexRequest.setIndexName(INDEX_NAME);
            DescribeSearchIndexResponse describeSearchIndexResponse = client.describeSearchIndex(describeSearchIndexRequest);
            System.out.println("describe response: " + describeSearchIndexResponse.jsonize());

            //等待权重设置生效
            try {
                Thread.sleep(50000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //此时，会有20%的查询被重建索引分担，逐步调大比例，可以灰度地将查询流量切到重建索引
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

        {   //设置查询流量的权重：源索引20%, 重建索引80%
            UpdateSearchIndexRequest updateSearchIndexRequest = new UpdateSearchIndexRequest(TABLE_NAME, INDEX_NAME,
                    Arrays.asList(
                            new QueryFlowWeight(INDEX_NAME, 20),
                            new QueryFlowWeight(INDEX_NAME_SCHEMA_MODIFIED, 80)
                    ));
            client.updateSearchIndex(updateSearchIndexRequest);

            //检查权重设置是否成功
            DescribeSearchIndexRequest describeSearchIndexRequest = new DescribeSearchIndexRequest();
            describeSearchIndexRequest.setTableName(TABLE_NAME);
            describeSearchIndexRequest.setIndexName(INDEX_NAME);
            DescribeSearchIndexResponse describeSearchIndexResponse = client.describeSearchIndex(describeSearchIndexRequest);
            System.out.println("describe response: " + describeSearchIndexResponse.jsonize());

            //等待权重设置生效
            try {
                Thread.sleep(50000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //此时，会有80%的查询被重建索引分担
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

        {   //设置查询流量的权重：源索引20%, 重建索引80%
            UpdateSearchIndexRequest updateSearchIndexRequest = new UpdateSearchIndexRequest(TABLE_NAME, INDEX_NAME,
                    Arrays.asList(
                            new QueryFlowWeight(INDEX_NAME, 0),
                            new QueryFlowWeight(INDEX_NAME_SCHEMA_MODIFIED, 100)
                    ));
            client.updateSearchIndex(updateSearchIndexRequest);

            //检查权重设置是否成功
            DescribeSearchIndexRequest describeSearchIndexRequest = new DescribeSearchIndexRequest();
            describeSearchIndexRequest.setTableName(TABLE_NAME);
            describeSearchIndexRequest.setIndexName(INDEX_NAME);
            DescribeSearchIndexResponse describeSearchIndexResponse = client.describeSearchIndex(describeSearchIndexRequest);
            System.out.println("describe response: " + describeSearchIndexResponse.jsonize());

            //等待权重设置生效
            try {
                Thread.sleep(50000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //此时，全部查询流量被引流到重建索引
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

        //step 4. 经过一段时间充分的AB test后，可以切换索引
        UpdateSearchIndexRequest switchRequest = new UpdateSearchIndexRequest(TABLE_NAME, INDEX_NAME, INDEX_NAME_SCHEMA_MODIFIED);
        client.updateSearchIndex(switchRequest);

//        //如果发现问题，还有机会切回
//        switchRequest = new UpdateSearchIndexRequest(TABLE_NAME, INDEX_NAME, INDEX_NAME_SCHEMA_MODIFIED);
//        client.updateSearchIndex(switchRequest);

        //step 5. 经过一段静默时间后，可以删除修改前的索引
        DeleteSearchIndexRequest deleteRequest = new DeleteSearchIndexRequest();
        deleteRequest.setTableName(TABLE_NAME);
        deleteRequest.setIndexName(INDEX_NAME_SCHEMA_MODIFIED);
        client.deleteSearchIndex(deleteRequest);
    }

    /**
     * 在 MatchQuery 的结果上对某一个字段求最小值。
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
        //使用结果
        System.out.println(resp.getAggregationResults().getAsMinAggregationResult("SomeName1").getValue());
    }

    /**
     * 在 MatchQuery 的结果上，对不同的字段求最小值、最大值、平均值、和、数量、去重的数量。
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
        //使用结果
        AggregationResults results = resp.getAggregationResults();

        System.out.println(results.getAsMinAggregationResult("SomeName1").getValue());
        System.out.println(results.getAsMaxAggregationResult("SomeName2").getValue());
        System.out.println(results.getAsSumAggregationResult("SomeName3").getValue());
        System.out.println(results.getAsAvgAggregationResult("SomeName4").getValue());
        System.out.println(results.getAsCountAggregationResult("SomeName5").getValue());
        System.out.println(results.getAsDistinctCountAggregationResult("SomeName6").getValue());
    }

    /**
     * 在 MatchQuery 的结果上，根据一个字段进行分组统计。
     * 举例：库存账单里有“篮球”、“足球”、“羽毛球”等，对这一个字段进行 GroupByField 聚合，返回： “篮球：10个”，“足球：5个”，“网球：1个”这样的聚合信息。
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
        //使用结果
        GroupByFieldResult results = resp.getGroupByResults().getAsGroupByFieldResult("someName1");
        //循环取出结果
        for (GroupByFieldResultItem item : results.getGroupByFieldResultItems()) {
            System.out.println("key：" + item.getKey());
            System.out.println("数量：" + item.getRowCount());
        }
    }

    /**
     * 在 MatchQuery 的结果上，根据给出的range范围进行分组统计。
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
        //使用结果
        GroupByRangeResult results = resp.getGroupByResults().getAsGroupByRangeResult("someName1");
        //循环取出结果
        for (GroupByRangeResultItem item : results.getGroupByRangeResultItems()) {
            System.out.println("数量：" + item.getRowCount());
        }
    }

    /**
     * 在 MatchQuery 的结果上，根据地理上的经纬度进行分组统计。
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
                        .origin(8.6545, 176.31231)   // 经纬度中心坐标点
                        .addRange(Double.MIN_VALUE, 100)
                        .addRange(100, 500)
                        .addRange(500, Double.MAX_VALUE))
                    .build())
            .build();
        SearchResponse resp = client.search(searchRequest);
        //使用结果
        GroupByRangeResult results = resp.getGroupByResults().getAsGroupByRangeResult("someName1");
        //循环取出结果
        for (GroupByRangeResultItem item : results.getGroupByRangeResultItems()) {
            System.out.println("数量：" + item.getRowCount());
        }
    }

    /**
     * 在 MatchQuery 的结果上，根据 filter 进行分组统计。
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
        //使用结果
        GroupByFilterResult results = resp.getGroupByResults().getAsGroupByFilterResult("someName1");
        //循环取出结果
        for (GroupByFilterResultItem item : results.getGroupByFilterResultItems()) {
            System.out.println("数量：" + item.getRowCount());
        }
    }

    /**
     * 在 MatchQuery 的结果上，进行各种agg和GroupBy组合，并支持多层嵌套（subAgg和subGroupBy）。
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
        //第一层的agg结果
        AggregationResults aggResults = resp.getAggregationResults();
        System.out.println(aggResults.getAsMinAggregationResult("SomeAggName1").getValue());
        System.out.println(aggResults.getAsMaxAggregationResult("SomeAggName2").getValue());

        //取出第一层的groupByField结果，并同时取出其嵌套的agg结果
        GroupByFieldResult results = resp.getGroupByResults().getAsGroupByFieldResult("someName1");
        for (GroupByFieldResultItem item : results.getGroupByFieldResultItems()) {
            System.out.println("数量：" + item.getRowCount());
            System.out.println("key：" + item.getKey());
            System.out.println(item.getSubAggregationResults().getAsMaxAggregationResult("subAgg1"));
            System.out.println(item.getSubAggregationResults().getAsSumAggregationResult("subAgg2"));
        }

        // 取出第一层没有嵌套的GroupByRange和GroupByFilter的结果
        //GroupByFilter
        GroupByFilterResult results1 = resp.getGroupByResults().getAsGroupByFilterResult("someName3");
        for (GroupByFilterResultItem item : results1.getGroupByFilterResultItems()) {
            System.out.println("数量：" + item.getRowCount());
        }

        //GroupByRange
        GroupByRangeResult results2 = resp.getGroupByResults().getAsGroupByRangeResult("someName2");
        for (GroupByRangeResultItem item : results2.getGroupByRangeResultItems()) {
            System.out.println("数量：" + item.getRowCount());
            System.out.println("from：" + item.getFrom());
            System.out.println("to：" + item.getTo());
        }
    }


    /**
     * 获取数据分块信息
     * 作用：用以{@link ScanQuery}中设置最大并行度{@link ScanQuery#setMaxParallel(Integer)}和sessionId {@link ParallelScanRequest#setSessionId(byte[])}
     */
    public static ComputeSplitsResponse computeSplits(SyncClient client, String tableName, String indexName){
        ComputeSplitsRequest computeSplitsRequest = new ComputeSplitsRequest();
        computeSplitsRequest.setTableName(tableName);
        computeSplitsRequest.setSplitsOptions(new SearchIndexSplitsOptions(indexName));
        return client.computeSplits(computeSplitsRequest);
    }



    /**
     * ScanQuery获取数据，原生接口使用示例，推荐 {@link SyncClientInterface#createParallelScanIterator(ParallelScanRequest)}.
     */
    public static void scanQuery(SyncClient client, String tableName, String indexName){
        // 计算分裂数和创建session
        ComputeSplitsResponse computeSplitsResponse = computeSplits(client, tableName, indexName);
        // 根据上一步创建的session进行获取数据
        ParallelScanRequest parallelScanRequest = ParallelScanRequest.newBuilder()
            .tableName(tableName)
            .indexName(indexName)
            .scanQuery(ScanQuery.newBuilder()
                .query(QueryBuilders.range("col_long").lessThan(123)) //这里的query决定了取得什么数据
                .limit(100)
                .build())
            .addColumnsToGet("col_bool", "col_keyword", "col_long")  //只能是索引中的字段
            .sessionId(computeSplitsResponse.getSessionId())
            .build();

        ParallelScanResponse response = client.parallelScan(parallelScanRequest);

        int total = 0;
        //持续取数据和消费数据
        while (null != response.getNextToken()) {
            // 取得数据，进行消费
            List<Row> rows = response.getRows();
            total += rows.size();

            //下一次请求的初始化
            parallelScanRequest.getScanQuery().setToken(response.getNextToken());
            response = client.parallelScan(parallelScanRequest);
        }
        System.out.println("原生接口 实际获取到的数据总数:" + total);
    }


    /**
     * ScanQuery获取数据，通过 {@link SyncClientInterface#createParallelScanIterator(ParallelScanRequest)}.
     */
    public static void scanQueryByRowIterator(SyncClient client, String tableName, String indexName){
        // 计算分裂数和创建session
        ComputeSplitsResponse computeSplitsResponse = computeSplits(client, tableName, indexName);
        // 根据上一步创建的session进行获取数据
        ParallelScanRequest parallelScanRequest = ParallelScanRequest.newBuilder()
            .tableName(tableName)
            .indexName(indexName)
            .scanQuery(ScanQuery.newBuilder()
                .query(QueryBuilders.range("col_long").lessThan(123)) //这里的query决定了取得什么数据
                .limit(100)
                .build())
            .addColumnsToGet("col_bool", "col_keyword", "col_long")  //只能是索引中的字段
            .sessionId(computeSplitsResponse.getSessionId())
            .build();

        RowIterator ltr = client.createParallelScanIterator(parallelScanRequest);

        int count = 0;
        while (ltr.hasNext()) {
            Row next = ltr.next();
            count++;
        }
        System.out.println("实际获取到的数据总数:" + count);
    }

    /**
     * 根据给出的fieldRange范围进行直方图统计。
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
                        .addFieldRange(1.0, 5.0))
                    .build())
            .build();
        SearchResponse resp = client.search(searchRequest);
        //使用结果
        GroupByHistogramResult results = resp.getGroupByResults().getAsGroupByHistogramResult("someName1");
        //循环取出结果
        for (GroupByHistogramItem item : results.getGroupByHistogramItems()) {
            System.out.println("key：" + item.getKey().asDouble() + " value:" + item.getValue());
        }
    }

    /**
     * 在 MatchQuery 的结果上进行百分比统计。
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

        //使用结果
        PercentilesAggregationResult percentilesAggregationResult = resp.getAggregationResults().getAsPercentilesAggregationResult(
            "percentilesAgg");
        for (PercentilesAggregationItem item : percentilesAggregationResult.getPercentilesAggregationItems()) {
            System.out.println("key：" + item.getKey() + " value:" + item.getValue().asDouble());
        }
    }

    /**
     * 在 MatchQuery 的结果上进行百分比统计。
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

        //使用结果
        PercentilesAggregationResult percentilesAggregationResult = resp.getAggregationResults().getAsPercentilesAggregationResult(
            "percentilesAgg");
        for (PercentilesAggregationItem item : percentilesAggregationResult.getPercentilesAggregationItems()) {
            System.out.println("key：" + item.getKey() + " value:" + item.getValue().asLong());
        }
    }

    /**
     * ScanQuery获取数据，遇到异常进行重试。
     */
    public static void scanQueryByRowIteratorWithException(SyncClient client, String tableName, String indexName){
        try {
            scanQueryByRowIterator(client, tableName, indexName);
        } catch (TableStoreException ex) {
            if (ex.getErrorCode().equals("OTSSessionExpired")){
                // session失效，进行重试
                scanQueryByRowIterator(client, tableName, indexName);
            }else {
                ex.printStackTrace();
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    /**
     * ScanQuery获取数据，使用多线程并行取数据
     */
    public static void scanQueryByMultiThread(SyncClient client) throws InterruptedException {
        // 计算分裂数和创建session
        ComputeSplitsResponse computeSplitsResponse = computeSplits(client, "tableName", "indexName");

        final class ThreadForScanQuery extends Thread {
            private SyncClient client;
            private ParallelScanRequest parallelScanRequest;

            private ThreadForScanQuery(SyncClient client, int currentParallelId, int maxParallel, byte[] sessionId) {
                this.client = client;
                this.setName(maxParallel + "-" + currentParallelId);  //设置线程名字
                this.parallelScanRequest = ParallelScanRequest.newBuilder()
                    .tableName("tableName")
                    .indexName("indexName")
                    .scanQuery(ScanQuery.newBuilder()
                        .query(QueryBuilders.range("col_long").lessThan(123)) //这里的query决定了取得什么数据
                        .limit(100)  //每次网络请求获取多少数据
                        .currentParallelId(currentParallelId)
                        .maxParallel(maxParallel)
                        .build())
                    .addColumnsToGet("col_bool", "col_keyword", "col_long")  //只能是索引中的字段
                    .sessionId(sessionId)
                    .build();
            }
            @Override
            public void run() {
                RowIterator ltr = client.createParallelScanIterator(parallelScanRequest);
                System.out.println("线程名字:" + this.getName());

                // 消费数据
                int count = 0;
                while (ltr.hasNext()) {
                    Row next = ltr.next(); //增加自己的处理逻辑
                    count++;
                }
                System.out.println("线程名字:" + this.getName() + ", 实际获取到的数据总数:" + count);
            }
        }

        // 最大线程数，不推荐超过maxParallel，性能可能会下降很多
        int maxParallel = computeSplitsResponse.getSplitsSize();
        byte[] sessionId = computeSplitsResponse.getSessionId();

        //多个线程一起跑, currentParallelId 取值范围是 [0, maxParallel)
        List<ThreadForScanQuery> threadList = new ArrayList<ThreadForScanQuery>();
        for (int i = 0; i < maxParallel; i++) {
            ThreadForScanQuery thread = new ThreadForScanQuery(client, i, maxParallel, sessionId);
            threadList.add(thread);
        }

        // 启动
        for (ThreadForScanQuery thread : threadList) {
            thread.start();
        }

        // 阻塞等待
        for (ThreadForScanQuery thread : threadList) {
            thread.join();
        }
        System.out.println("all thread done!");
    }
}
