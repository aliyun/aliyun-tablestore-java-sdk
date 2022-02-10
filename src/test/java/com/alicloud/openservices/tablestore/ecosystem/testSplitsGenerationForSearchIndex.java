package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.iterator.RowIterator;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


public class testSplitsGenerationForSearchIndex {
    private static SyncClient client;
    public static ComputeParameters para;
    private static final Gson GSON = new Gson();

    public static String endpoint = "http://iquandata.ali-cn-hangzhou.ots.aliyuncs.com";
    public static String accessKey = "";
    public static String secret = "";
    public static String instanceName = "iquandata";
    public static String tableName = "wenxian_searchIndex_spark_test";
    public static String indexName = "wenxian_searchIndex_spark_test_index2";

    public static Filter filterPk1StartWith = new Filter(Filter.CompareOperator.START_WITH, "pk1", ColumnValue.fromString("0000pppeee4444"));
    public static Filter filterPk1Equal = new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromString("0000pppeee4444tttiii8888xxxmmm"));
    public static Filter filterPk1NotEqual = new Filter(Filter.CompareOperator.NOT_EQUAL, "pk1", ColumnValue.fromString("0000pppeee4444tttiii8888xxxmmm"));
    public static Filter filterIsNull = new Filter(Filter.CompareOperator.IS_NULL, "val_long1");
    public static Filter filterLessThan = new Filter(Filter.CompareOperator.LESS_THAN, "val_long1", ColumnValue.fromLong(98350700));
    public static Filter filterGreaterThan = new Filter(Filter.CompareOperator.GREATER_THAN, "val_long1", ColumnValue.fromLong(3697900));
    public static Filter filterLessTanOrEqual = new Filter(Filter.CompareOperator.LESS_EQUAL, "val_long1", ColumnValue.fromLong(98350700));
    public static Filter filterGreaterThanOrEqual = new Filter(Filter.CompareOperator.GREATER_EQUAL, "val_long1", ColumnValue.fromLong(3697900));
    public static Filter filterGeoDistance = new Filter(Filter.CompareOperator.EQUAL, "val_geo", ColumnValue.fromString("{\"centerPoint\":\"6,9\", \"distanceInMeter\": 100000}"));
    public static Filter filterGeoBoundingBox = new Filter(Filter.CompareOperator.EQUAL, "val_geo", ColumnValue.fromString("{\"topLeft\":\"3,0\", \"bottomRight\": \"0,3\"}"));
    public static Filter filterGeoPolygon = new Filter(Filter.CompareOperator.EQUAL, "val_geo", ColumnValue.fromString("{\"points\":[\"5,0\",\"5,1\", \"6,1\", \"6,10\"]}"));


    public static ArrayList<String> requiredColumns = new ArrayList<String>();

    @BeforeClass
    public static void classBefore() throws JsonSyntaxException, IOException {
        ClientConfiguration conf = new ClientConfiguration();
        client = new SyncClient(
                endpoint,
                accessKey,
                secret,
                instanceName,
                conf);
        requiredColumns.add("pk1");
        requiredColumns.add("val_keyword1");
        requiredColumns.add("val_keyword2");
        requiredColumns.add("val_keyword3");
        requiredColumns.add("val_bool");
        requiredColumns.add("val_double");
        requiredColumns.add("val_long1");
        requiredColumns.add("val_long2");
        requiredColumns.add("val_text");
        requiredColumns.add("val_geo");

        para = new ComputeParameters(indexName, 16);
    }

    @AfterClass
    public static void classAfter() {
        client.shutdown();
    }


    @Test
    //Or
    public void testOrFilter() {
        resetOrGeoFlag();
        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(filterPk1Equal);
        filters.add(filterPk1StartWith);
        Filter filter = new Filter(Filter.LogicOperator.OR, filters);

        //通过Filter构造rowDataHashMap
        HashMap<PrimaryKey, Row> rowDataHashMap = rowDataMapFromFilter(filter);

        //手动构造searchQuery(Expected)
        SearchQuery.Builder builder = SearchQuery.newBuilder();
        BoolQuery.Builder boolBuilder = QueryBuilders.bool();
        boolBuilder.should(QueryBuilders.prefix(filterPk1StartWith.getColumnName(), filterPk1StartWith.getColumnValue().asString()));
        boolBuilder.should(QueryBuilders.term(filterPk1Equal.getColumnName(), filterPk1Equal.getColumnValue().asString()));
        SearchRequest searchRequest = new SearchRequest(tableName, indexName, builder.query(boolBuilder).build());
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(requiredColumns);
        searchRequest.setColumnsToGet(columnsToGet);
        RowIterator searchIterator = client.createSearchIterator(searchRequest);

        //用filter构造的rowDataHashMap来核对searchQuery取到的数据
        verifyDataFromSearchIndexByMapFromFilter(rowDataHashMap, searchIterator);
    }

    @Test
    //And
    public void testAndFilter() {
        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(filterPk1Equal);
        filters.add(filterPk1StartWith);
        Filter filter = new Filter(Filter.LogicOperator.AND, filters);

        //通过Filter构造rowDataHashMap
        HashMap<PrimaryKey, Row> rowDataHashMap = rowDataMapFromFilter(filter);

        //手动构造searchQuery(Expected)
        SearchQuery.Builder builder = SearchQuery.newBuilder();
        BoolQuery.Builder boolBuilder = QueryBuilders.bool();
        boolBuilder.must(QueryBuilders.prefix(filterPk1StartWith.getColumnName(), filterPk1StartWith.getColumnValue().asString()));
        boolBuilder.must(QueryBuilders.term(filterPk1Equal.getColumnName(), filterPk1Equal.getColumnValue().asString()));
        SearchRequest searchRequest = new SearchRequest(tableName, indexName, builder.query(boolBuilder).build());
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(requiredColumns);
        searchRequest.setColumnsToGet(columnsToGet);
        RowIterator searchIterator = client.createSearchIterator(searchRequest);

        //用filter构造的rowDataHashMap来核对searchQuery取到的数据
        verifyDataFromSearchIndexByMapFromFilter(rowDataHashMap, searchIterator);
    }


    @Test
    //NOT 与 NOTEQUAL
    public void testNotNotequalFilter() {
        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(filterPk1NotEqual);
        filters.add(filterPk1StartWith);
        Filter filter = new Filter(Filter.LogicOperator.AND, filters);

        //通过Filter构造rowDataHashMap
        HashMap<PrimaryKey, Row> rowDataHashMap = rowDataMapFromFilter(filter);

        //手动构造searchQuery(Expected)
        SearchQuery.Builder builder = SearchQuery.newBuilder();
        BoolQuery.Builder boolBuilder = QueryBuilders.bool();
        boolBuilder.must(QueryBuilders.prefix(filterPk1StartWith.getColumnName(), filterPk1StartWith.getColumnValue().asString()));
        boolBuilder.mustNot(QueryBuilders.term(filterPk1NotEqual.getColumnName(), filterPk1NotEqual.getColumnValue().asString()));
        SearchRequest searchRequest = new SearchRequest(tableName, indexName, builder.query(boolBuilder).build());
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(requiredColumns);
        searchRequest.setColumnsToGet(columnsToGet);
        RowIterator searchIterator = client.createSearchIterator(searchRequest);

        //用filter构造的rowDataHashMap来核对searchQuery取到的数据
        verifyDataFromSearchIndexByMapFromFilter(rowDataHashMap, searchIterator);
    }

    @Test
    //Equal
    public void testEqualFilter() {
        //构造Filter
        String fieldName = "val_long1";
        ColumnValue fieldValue = ColumnValue.fromLong(3697900);
        Filter filter = new Filter(Filter.CompareOperator.EQUAL, fieldName, fieldValue);

        //通过Filter构造rowDataHashMap
        HashMap<PrimaryKey, Row> rowDataHashMap = rowDataMapFromFilter(filter);

        //手动构造searchQuery(Expected)
        SearchQuery searchQuery = new SearchQuery();
        TermQuery termQuery = new TermQuery();
        termQuery.setFieldName(fieldName);
        termQuery.setTerm(fieldValue);
        searchQuery.setQuery(termQuery);
        SearchRequest searchRequest = new SearchRequest(tableName, indexName, searchQuery);
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(requiredColumns);
        searchRequest.setColumnsToGet(columnsToGet);
        RowIterator searchIterator = client.createSearchIterator(searchRequest);

        //用filter构造的rowDataHashMap来核对searchQuery取到的数据
        verifyDataFromSearchIndexByMapFromFilter(rowDataHashMap, searchIterator);
    }

    @Test
    //IS NULL
    //TODO 除了5.10 以上的pulic实例  其他requireColumn的列如果为null 则行会被过滤掉 所以这里自定义pkList作为requireColumn

    public void testIsNullFilter() {
        ArrayList<String> pkList = new ArrayList<String>();
        pkList.add("pk1");


        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(filterIsNull);
        Filter filter = new Filter(Filter.LogicOperator.AND, filters);

        //通过Filter构造rowDataHashMap
        HashMap<PrimaryKey, Row> rowDataHashMap = new HashMap<PrimaryKey, Row>();
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(client);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(client, filter, tableName, para,
                pkList);

        for (ITablestoreSplit split : splits) {
            split.initial(client);
            Iterator<Row> rowIterator = split.getRowIterator(client);
            while (rowIterator.hasNext()) {
                Row next = rowIterator.next();
                rowDataHashMap.put(next.getPrimaryKey(), next);
            }
        }

        //手动构造searchQuery(Expected)
        SearchQuery.Builder builder = SearchQuery.newBuilder();
        ExistsQuery.Builder existsBuilder = QueryBuilders.exists(filterIsNull.getColumnName());
        BoolQuery.Builder boolBuilder = QueryBuilders.bool().mustNot(existsBuilder);
        SearchRequest searchRequest = new SearchRequest(tableName, indexName, builder.query(boolBuilder).build());
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(pkList);
        searchRequest.setColumnsToGet(columnsToGet);
        RowIterator searchIterator = client.createSearchIterator(searchRequest);

        //用filter构造的rowDataHashMap来核对searchQuery取到的数据
        verifyDataFromSearchIndexByMapFromFilter(rowDataHashMap, searchIterator);
    }

    //In
    @Test
    public void testInFilter() {
        //构造Filter
        String fieldName = "val_long1";
        ColumnValue fieldValue1 = ColumnValue.fromLong(3697900);
        ColumnValue fieldValue2 = ColumnValue.fromLong(99871300);
        ArrayList<ColumnValue> columnValues = new ArrayList<ColumnValue>();
        columnValues.add(fieldValue1);
        columnValues.add(fieldValue2);
        Filter filter = new Filter(Filter.CompareOperator.IN, fieldName, columnValues);

        //通过Filter构造rowDataHashMap
        HashMap<PrimaryKey, Row> rowDataHashMap = rowDataMapFromFilter(filter);

        //手动构造searchQuery(Expected)
        SearchQuery searchQuery = new SearchQuery();
        TermsQuery termsQuery = new TermsQuery();
        termsQuery.setFieldName(fieldName);
        termsQuery.setTerms(columnValues);
        searchQuery.setQuery(termsQuery);
        SearchRequest searchRequest = new SearchRequest(tableName, indexName, searchQuery);
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(requiredColumns);
        searchRequest.setColumnsToGet(columnsToGet);
        RowIterator searchIterator = client.createSearchIterator(searchRequest);

        //用filter构造的rowDataHashMap来核对searchQuery取到的数据
        verifyDataFromSearchIndexByMapFromFilter(rowDataHashMap, searchIterator);

    }

    @Test
    //LessThan  GreaterThan
    public void testRangeFilter() {
        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(filterLessThan);
        filters.add(filterGreaterThan);
        filters.add(filterPk1StartWith);
        Filter filter = new Filter(Filter.LogicOperator.AND, filters);

        //通过Filter构造rowDataHashMap
        HashMap<PrimaryKey, Row> rowDataHashMap = rowDataMapFromFilter(filter);

        //手动构造searchQuery(Expected)
        SearchQuery.Builder builder = SearchQuery.newBuilder();
        BoolQuery.Builder boolBuilder = QueryBuilders.bool().must(QueryBuilders.prefix(filterPk1StartWith.getColumnName(), filterPk1StartWith.getColumnValue().asString())
        ).must(QueryBuilders.range(filterLessThan.getColumnName()).lessThan(filterLessThan.getColumnValue().getValue())
        ).must(QueryBuilders.range(filterGreaterThan.getColumnName()).greaterThan(filterGreaterThan.getColumnValue().getValue()));

        SearchRequest searchRequest = new SearchRequest(tableName, indexName, builder.query(boolBuilder).build());
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(requiredColumns);
        searchRequest.setColumnsToGet(columnsToGet);
        RowIterator searchIterator = client.createSearchIterator(searchRequest);

        //用filter构造的rowDataHashMap来核对searchQuery取到的数据
        verifyDataFromSearchIndexByMapFromFilter(rowDataHashMap, searchIterator);
    }

    @Test
    //LessThanEqual  GreaterThanEqual
    public void testRangeEqualFilter() {
        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(filterLessTanOrEqual);
        filters.add(filterGreaterThanOrEqual);
        filters.add(filterPk1StartWith);
        Filter filter = new Filter(Filter.LogicOperator.AND, filters);

        //通过Filter构造rowDataHashMap
        HashMap<PrimaryKey, Row> rowDataHashMap = rowDataMapFromFilter(filter);

        //手动构造searchQuery(Expected)
        SearchQuery.Builder builder = SearchQuery.newBuilder();
        BoolQuery.Builder boolBuilder = QueryBuilders.bool().must(QueryBuilders.prefix(filterPk1StartWith.getColumnName(), filterPk1StartWith.getColumnValue().asString())
        ).must(QueryBuilders.range(filterLessTanOrEqual.getColumnName()).lessThanOrEqual(filterLessTanOrEqual.getColumnValue().getValue())
        ).must(QueryBuilders.range(filterGreaterThanOrEqual.getColumnName()).greaterThanOrEqual(filterGreaterThanOrEqual.getColumnValue().getValue()));

        SearchRequest searchRequest = new SearchRequest(tableName, indexName, builder.query(boolBuilder).build());
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(requiredColumns);
        searchRequest.setColumnsToGet(columnsToGet);
        RowIterator searchIterator = client.createSearchIterator(searchRequest);

        //用filter构造的rowDataHashMap来核对searchQuery取到的数据
        verifyDataFromSearchIndexByMapFromFilter(rowDataHashMap, searchIterator);
    }


    @Test
    //GeoDistance
    public void testGeoDistanceFilter() {
        resetOrGeoFlag();
        //通过Filter构造rowDataHashMap
        HashMap<PrimaryKey, Row> rowDataHashMap = rowDataMapFromFilter(filterGeoDistance);

        GeoQueryHelper geoQueryHelper = GSON.fromJson(filterGeoDistance.getColumnValue().asString(), GeoQueryHelper.class);

        //手动构造searchQuery(Expected)
        SearchQuery searchQuery = new SearchQuery();
        GeoDistanceQuery geoDistanceQuery = new GeoDistanceQuery();
        geoDistanceQuery.setFieldName(filterGeoDistance.getColumnName());
        geoDistanceQuery.setCenterPoint(geoQueryHelper.getCenterPoint());
        geoDistanceQuery.setDistanceInMeter(geoQueryHelper.getDistanceInMeter());
        searchQuery.setQuery(geoDistanceQuery);
        SearchRequest searchRequest = new SearchRequest(tableName, indexName, searchQuery);
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(requiredColumns);
        searchRequest.setColumnsToGet(columnsToGet);
        RowIterator searchIterator = client.createSearchIterator(searchRequest);
        //用filter构造的rowDataHashMap来核对searchQuery取到的数据
        verifyDataFromSearchIndexByMapFromFilter(rowDataHashMap, searchIterator);
    }

    @Test
    //GeoBoundingBox
    public void testGeoBoundingBoxFilter() {
        //为了防止不同单元测试之间相互影响下面倆全局变量
        resetOrGeoFlag();


        //通过Filter构造rowDataHashMap
        HashMap<PrimaryKey, Row> rowDataHashMap = rowDataMapFromFilter(filterGeoBoundingBox);

        //手动构造searchQuery(Expected)
        GeoQueryHelper geoQueryHelper = GSON.fromJson(filterGeoBoundingBox.getColumnValue().asString(), GeoQueryHelper.class);
        SearchQuery searchQuery = new SearchQuery();
        GeoBoundingBoxQuery geoBoundingBoxQuery = new GeoBoundingBoxQuery();
        geoBoundingBoxQuery.setFieldName(filterGeoBoundingBox.getColumnName());
        geoBoundingBoxQuery.setTopLeft(geoQueryHelper.getTopLeft());
        geoBoundingBoxQuery.setBottomRight(geoQueryHelper.getBottomRight());
        searchQuery.setQuery(geoBoundingBoxQuery);
        SearchRequest searchRequest = new SearchRequest(tableName, indexName, searchQuery);
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(requiredColumns);
        searchRequest.setColumnsToGet(columnsToGet);
        RowIterator searchIterator = client.createSearchIterator(searchRequest);

        //用filter构造的rowDataHashMap来核对searchQuery取到的数据
        verifyDataFromSearchIndexByMapFromFilter(rowDataHashMap, searchIterator);
    }


    @Test
    //GeoPolygon
    public void testGeoPolygonFilter() {
        resetOrGeoFlag();
        //通过Filter构造rowDataHashMap
        HashMap<PrimaryKey, Row> rowDataHashMap = rowDataMapFromFilter(filterGeoPolygon);

        //手动构造searchQuery(Expected)
        SearchQuery searchQuery = new SearchQuery();
        GeoQueryHelper geoQueryHelper = GSON.fromJson(filterGeoPolygon.getColumnValue().asString(), GeoQueryHelper.class);
        GeoPolygonQuery geoPolygonQuery = new GeoPolygonQuery();
        geoPolygonQuery.setFieldName(filterGeoPolygon.getColumnName());
        geoPolygonQuery.setPoints(geoQueryHelper.getPoints());
        searchQuery.setQuery(geoPolygonQuery);
        SearchRequest searchRequest = new SearchRequest(tableName, indexName, searchQuery);
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(requiredColumns);
        searchRequest.setColumnsToGet(columnsToGet);
        RowIterator searchIterator = client.createSearchIterator(searchRequest);

        //用filter构造的rowDataHashMap来核对searchQuery取到的数据
        verifyDataFromSearchIndexByMapFromFilter(rowDataHashMap, searchIterator);
    }

    private HashMap<PrimaryKey, Row> rowDataMapFromFilter(Filter filter) {

        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(client);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(client, filter, tableName, para, requiredColumns);
        HashMap<PrimaryKey, Row> rowDataHashMap = new HashMap<PrimaryKey, Row>();

        for (ITablestoreSplit split : splits) {
            split.initial(client);
            Iterator<Row> rowIterator = split.getRowIterator(client);
            while (rowIterator.hasNext()) {
                Row next = rowIterator.next();
                rowDataHashMap.put(next.getPrimaryKey(), next);
            }
        }

        return rowDataHashMap;
    }

    private void verifyDataFromSearchIndexByMapFromFilter(HashMap<PrimaryKey, Row> rowDataHashMap, RowIterator rowIterator) {
        int searchIndexRowCount = 0;
        while (rowIterator.hasNext()) {
            searchIndexRowCount++;
            Row rowFromSearchIndex = rowIterator.next();
            PrimaryKey primaryKeyFromSearchIndex = rowFromSearchIndex.getPrimaryKey();
            Assert.assertTrue(rowDataHashMap.containsKey(primaryKeyFromSearchIndex));
            Row rowFromFilter = rowDataHashMap.get(primaryKeyFromSearchIndex);
            Column[] rowFromFilterColumns = rowFromFilter.getColumns();
            Column[] rowFromSearchIndexColumns = rowFromSearchIndex.getColumns();
            Assert.assertEquals(rowFromFilterColumns.length, rowFromSearchIndexColumns.length);
            List<String> geoColumnList = getGeoColumnList();
            for (int i = 0; i < rowFromFilterColumns.length; i++) {
                Column rowFromFilterColumn = rowFromFilterColumns[i];
                Column rowFromSearchIndexColumn = rowFromSearchIndexColumns[i];
                ColumnValue rowFromFilterColumnValue = rowFromFilterColumn.getValue();
                ColumnValue rowFromSearchIndexColumnValue = rowFromSearchIndexColumn.getValue();
                //geo类型
                if (geoColumnList.contains(rowFromFilterColumn.getName())) {
                    //因为SearchQuery是全表扫到的数据  带timestamp,且geo数据是也是用户数据表里自己写得
                    // 而Filter转parallel scan的数据是索引直接返回的数据   没有timestamp,而且geo数据也是es格式化之后的
                    // 所以 timestamp字段跳过检验,es数据在此做格式化,再检验
                    String[] splitFilter = rowFromFilterColumnValue.asString().split(",");
                    String[] splitSearchIndex = rowFromSearchIndexColumnValue.asString().split(",");
                    for (int j = 0; j < splitFilter.length; j++) {
                        Assert.assertEquals(splitFilter[j].trim(), splitSearchIndex[j].trim());
                    }
                    //其他类型
                } else {
                    Assert.assertEquals(rowFromFilterColumnValue, rowFromSearchIndexColumnValue);
                }

            }
        }
        Assert.assertEquals(rowDataHashMap.size(), searchIndexRowCount);
        System.out.println(rowDataHashMap.size());
    }

    private List<String> getGeoColumnList() {
        DescribeSearchIndexRequest request = new DescribeSearchIndexRequest();
        request.setTableName(tableName);
        request.setIndexName(para.getSearchIndexName());
        DescribeSearchIndexResponse response = client.describeSearchIndex(request);
        IndexSchema schema = response.getSchema();
        List<String> geoFieldList = new ArrayList<String>();
        for (FieldSchema fieldSchema : schema.getFieldSchemas()) {
            if (fieldSchema.getFieldType() == FieldType.GEO_POINT) {
                geoFieldList.add(fieldSchema.getFieldName());
            }
        }
        return geoFieldList;
    }

    private void resetOrGeoFlag() {
        TablestoreSplit.containOr = false;
        TablestoreSplit.containGeo = false;
    }

}
