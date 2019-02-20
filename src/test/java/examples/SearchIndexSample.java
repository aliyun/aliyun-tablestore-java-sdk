package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;

import java.util.Arrays;
import java.util.List;

public class SearchIndexSample {

    /**
     * 本示例中建立一张表,名为search_index_sample_table,两个主键, 主键分别为pk1，pk2.
     * 给这张表建立一个SearchIndex，然后列出表下的SearchIndex，然后查询SearchIndex的信息。
     * 向表内写入几条数据，并通过几种search query进行查询。
     *
     */
    private static final String TABLE_NAME = "search_index_sample_table";
    private static final String INDEX_NAME = "test_index";
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
            waitUntilAllDataSync(client, 5);

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
     * @param client
     */
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
        while (resp.getNextToken()!=null) {
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
    }

    private static void deleteTable(SyncClient client) {
        DeleteTableRequest request = new DeleteTableRequest(TABLE_NAME);
        client.deleteTable(request);
    }

    private static void putRow(SyncClient client) {
        String[] keywords = {"hangzhou", "beijing", "shanghai", "hangzhou shanghai", "hangzhou beijing shanghai"};
        long[] longValues = {1, 2, 3, 4, 5};
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
            client.putRow(new PutRowRequest(rowPutChange));
        }
    }

    private static void waitUntilAllDataSync(SyncClient client, long expectTotalHit) {
        long begin = System.currentTimeMillis();
        while (true) {
            SearchQuery searchQuery = new SearchQuery();
            searchQuery.setQuery(new MatchAllQuery());
            searchQuery.setGetTotalCount(true);
            SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);
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
     * @param client
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
     * @param client
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

        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setReturnAll(true); // 设置返回所有列
        searchRequest.setColumnsToGet(columnsToGet);

        resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }


    /**
     * 查询表中Col_Text这一列的值能够匹配"hangzhou shanghai"的数据，匹配条件为短语匹配(要求短语完整的按照顺序匹配)，返回匹配到的总行数和一些匹配成功的行。
     * @param client
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

        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setReturnAll(true); // 设置返回所有列
        searchRequest.setColumnsToGet(columnsToGet);

        resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 查询表中Col_Keyword这一列精确匹配"hangzhou"的数据。
     * @param client
     */
    private static void termQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        TermQuery termQuery = new TermQuery(); // 设置查询类型为TermQuery
        termQuery.setFieldName("Col_Keyword"); // 设置要匹配的字段
        termQuery.setTerm(ColumnValue.fromString("hangzhou")); // 设置要匹配的值
        searchQuery.setQuery(termQuery);
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setReturnAll(true); // 设置返回所有列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 查询表中Col_Keyword这一列精确匹配"hangzhou"或"shanghai"的数据。
     * TermsQuery可以使用多个Term同时查询。
     * @param client
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

        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setReturnAll(true); // 设置返回所有列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 查询表中Col_Keyword这一列前缀为"hangzhou"的数据。
     * @param client
     */
    private static void prefixQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        PrefixQuery prefixQuery = new PrefixQuery(); // 设置查询类型为PrefixQuery
        prefixQuery.setFieldName("Col_Keyword");
        prefixQuery.setPrefix("hangzhou");
        searchQuery.setQuery(prefixQuery);
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setReturnAll(true); // 设置返回所有列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 使用通配符查询，查询表中Col_Keyword这一列的值匹配"hang*u"的数据
     * @param client
     */
    private static void wildcardQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        WildcardQuery wildcardQuery = new WildcardQuery(); // 设置查询类型为WildcardQuery
        wildcardQuery.setFieldName("Col_Keyword");
        wildcardQuery.setValue("hang*u"); //wildcardQuery支持通配符
        searchQuery.setQuery(wildcardQuery);
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setReturnAll(true); // 设置返回所有列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);

        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 查询表中Col_Long这一列大于3的数据，结果按照Col_Long这一列的值逆序排序。
     * @param client
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
     * @param client
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

        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(Arrays.asList("Col_GeoPoint"));  //设置返回Col_GeoPoint这一列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 查询表中Col_GeoPoint这一列的值距离中心点不超过一定距离的数据。
     * @param client
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

        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(Arrays.asList("Col_GeoPoint"));  //设置返回Col_GeoPoint这一列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 查询表中Col_GeoPoint这一列的值在一个给定多边形范围内的数据。
     * @param client
     */
    public static void geoPolygonQuery(SyncClient client) {
        SearchQuery searchQuery = new SearchQuery();
        GeoPolygonQuery geoPolygonQuery = new GeoPolygonQuery();  // 设置查询类型为GeoPolygonQuery
        geoPolygonQuery.setFieldName("Col_GeoPoint");
        geoPolygonQuery.setPoints(Arrays.asList("0,0","5,5","5,0")); // 设置多边形的顶点
        searchQuery.setQuery(geoPolygonQuery);
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(TABLE_NAME, INDEX_NAME, searchQuery);

        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(Arrays.asList("Col_GeoPoint"));  //设置返回Col_GeoPoint这一列
        searchRequest.setColumnsToGet(columnsToGet);

        SearchResponse resp = client.search(searchRequest);
        System.out.println("TotalCount: " + resp.getTotalCount()); // 匹配到的总行数，非返回行数
        System.out.println("Row: " + resp.getRows());
    }

    /**
     * 通过BoolQuery进行复合条件查询。
     * @param client
     */
    public static void boolQuery(SyncClient client) {
        /**
         * 查询条件一：RangeQuery，Col_Long这一列的值要大于3
         */
        RangeQuery rangeQuery = new RangeQuery();
        rangeQuery.setFieldName("Col_Long");
        rangeQuery.greaterThan(ColumnValue.fromLong(3));

        /**
         * 查询条件二：MatchQuery，Col_Keyword这一列的值要匹配"hangzhou"
         */
        MatchQuery matchQuery = new MatchQuery(); // 设置查询类型为MatchQuery
        matchQuery.setFieldName("Col_Keyword"); // 设置要匹配的字段
        matchQuery.setText("hangzhou"); // 设置要匹配的值

        SearchQuery searchQuery = new SearchQuery();
        {
            /**
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
            /**
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

}
