package com.alicloud.openservices.tablestore.model.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.core.utils.Repeat;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.search.SearchRequest.ColumnsToGet;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilders;
import com.alicloud.openservices.tablestore.model.search.agg.SumAggregation;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByBuilders;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByField;
import com.alicloud.openservices.tablestore.model.search.query.MatchAllQuery;
import com.alicloud.openservices.tablestore.model.search.query.QueryBuilders;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;
import org.junit.Assert;
import org.junit.Test;

/**
 * SearchRequest Tester.
 *
 * @author <Authors xunjian>
 * @version 1.0
 * @since <pre>Sep 29, 2019</pre>
 */
public class SearchRequestTest extends BaseSearchTest {

    /**
     * Method: getTableName()
     */
    @Test
    public void testGetTableName() {
        SearchRequest s1 = new SearchRequest();
        s1.setTableName("a1");
        SearchRequest s2 = SearchRequest.newBuilder().tableName("a1").build();
        Assert.assertEquals(s1.getTableName(), s2.getTableName());
        Assert.assertEquals(s1.getRequestInfo(false), s2.getRequestInfo(false));
        Assert.assertNotSame(s1.getRequestInfo(true), s2.getRequestInfo(false));
    }

    /**
     * Method: setTableName(String tableName)
     */
    @Test
    public void testSetTableName() {
        SearchRequest s1 = new SearchRequest();
        s1.setTableName("s2");
        SearchRequest s2 = SearchRequest.newBuilder().tableName("s2").build();
        Assert.assertEquals(s1.getTableName(), s2.getTableName());
        Assert.assertEquals(s1.getRequestInfo(false), s2.getRequestInfo(false));
        Assert.assertNotSame(s1.getRequestInfo(true), s2.getRequestInfo(false));
    }

    /**
     * Method: getIndexName()
     */
    @Test
    public void testGetIndexName() {
        SearchRequest s1 = new SearchRequest();
        s1.setTableName("a1");
        s1.setIndexName("i1");
        SearchRequest s2 = SearchRequest.newBuilder().tableName("a1").indexName("i1").build();
        Assert.assertEquals(s1.getTableName(), s2.getTableName());
        Assert.assertEquals(s1.getRequestInfo(false), s2.getRequestInfo(false));
        Assert.assertNotSame(s1.getRequestInfo(true), s2.getRequestInfo(false));
    }

    /**
     * Method: setIndexName(String indexName)
     */
    @Test
    public void testSetIndexName() {
        SearchRequest s1 = new SearchRequest();
        s1.setTableName("a1");
        s1.setIndexName("i1");
        SearchRequest s2 = SearchRequest.newBuilder().tableName("a1").indexName("i1").build();
        Assert.assertEquals(s1.getIndexName(), s2.getIndexName());
        Assert.assertEquals(s1.getRequestInfo(false), s2.getRequestInfo(false));
        Assert.assertNotSame(s1.getRequestInfo(true), s2.getRequestInfo(false));
    }

    /**
     * Method: getSearchQuery()
     */
    @Test
    public void testGetSearchQuery() {
        SearchRequest s1 = new SearchRequest();
        s1.setSearchQuery(SearchQuery.newBuilder().query(QueryBuilders.matchAll()).build());
        SearchRequest s2 = new SearchRequest();
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(new MatchAllQuery());
        s2.setSearchQuery(searchQuery);
        Assert.assertEquals(s1.getSearchQuery().getQuery().getQueryType(),
            s2.getSearchQuery().getQuery().getQueryType());
        Assert.assertEquals(s1.getRequestInfo(false), s2.getRequestInfo(false));
        Assert.assertNotSame(s1.getRequestInfo(true), s2.getRequestInfo(false));
    }

    /**
     * Method: setSearchQuery(SearchQuery searchQuery)
     */
    @Test
    public void testSetSearchQuery() {
        SearchRequest s1 = new SearchRequest();
        s1.setSearchQuery(SearchQuery.newBuilder().query(QueryBuilders.range("123").greaterThanOrEqual(456)).build());
        SearchRequest s2 = new SearchRequest();
        SearchQuery searchQuery = new SearchQuery();
        RangeQuery query = new RangeQuery();
        query.setFieldName("123");
        query.setFrom(ColumnValue.fromLong(456), true);
        searchQuery.setQuery(query);
        s2.setSearchQuery(searchQuery);
        Assert.assertEquals(s1.getRequestInfo(false), s2.getRequestInfo(false));
        Assert.assertNotSame(s1.getRequestInfo(true), s2.getRequestInfo(false));
    }

    /**
     * Method: getColumnsToGet()
     */
    @Test
    public void testGetColumnsToGet() {
        SearchRequest s1 = SearchRequest.newBuilder()
            .addColumnsToGet("123", "456")
            .addColumnsToGet(Arrays.asList("ggg", "aaa"))
            .build();
        Assert.assertFalse(s1.getColumnsToGet().isReturnAll());
        Assert.assertEquals(s1.getColumnsToGet().getColumns().toString(),
            Arrays.asList("123", "456", "ggg", "aaa").toString());

    }

    /**
     * Method: setColumnsToGet(ColumnsToGet columnsToGet)
     */
    @Test
    public void testSetColumnsToGet() {
        SearchRequest s1 = new SearchRequest();
        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setReturnAll(true);
        s1.setColumnsToGet(columnsToGet);
        Assert.assertTrue(s1.getColumnsToGet().isReturnAll());
        columnsToGet.setColumns(Arrays.asList("13", "34", "ggg"));
        Assert.assertEquals(s1.getColumnsToGet().getColumns().toString(), Arrays.asList("13", "34", "ggg").toString());
    }

    /**
     * Method: getRoutingValues()
     */
    @Test
    public void testGetRoutingValues() {
        List<PrimaryKeyColumn> primaryKeyColumns = new ArrayList<PrimaryKeyColumn>();
        primaryKeyColumns.add(new PrimaryKeyColumn("111", PrimaryKeyValue.fromString("111")));
        primaryKeyColumns.add(new PrimaryKeyColumn("222", PrimaryKeyValue.fromString("222")));
        PrimaryKey primaryKey1 = new PrimaryKey(primaryKeyColumns);

        List<PrimaryKeyColumn> primaryKeyColumns2 = new ArrayList<PrimaryKeyColumn>();
        primaryKeyColumns2.add(new PrimaryKeyColumn("333", PrimaryKeyValue.fromString("111")));
        primaryKeyColumns2.add(new PrimaryKeyColumn("444", PrimaryKeyValue.fromString("222")));
        PrimaryKey primaryKey2 = new PrimaryKey(primaryKeyColumns2);

        SearchRequest s1 = new SearchRequest();
        s1.setRoutingValues(Arrays.asList(primaryKey1, primaryKey2));

        SearchRequest s2 = SearchRequest.newBuilder()
            .addRoutingValues(Collections.singletonList(primaryKey1))
            .addRoutingValues(Collections.singletonList(primaryKey2))
            .build();
        Assert.assertEquals(s1.getRequestInfo(false), s2.getRequestInfo(false));
        Assert.assertNotSame(s1.getRequestInfo(true), s2.getRequestInfo(false));
    }

    /**
     * Method: setRoutingValues(List<PrimaryKey> routingValues)
     */
    @Test
    public void testSetRoutingValues() {
        SearchRequest s1 = new SearchRequest();
        List<PrimaryKeyColumn> primaryKeyColumns = new ArrayList<PrimaryKeyColumn>();
        primaryKeyColumns.add(new PrimaryKeyColumn("111", PrimaryKeyValue.fromString("111")));
        primaryKeyColumns.add(new PrimaryKeyColumn("222", PrimaryKeyValue.fromString("222")));
        PrimaryKey primaryKey1 = new PrimaryKey(primaryKeyColumns);
        List<PrimaryKeyColumn> primaryKeyColumns2 = new ArrayList<PrimaryKeyColumn>();
        primaryKeyColumns2.add(new PrimaryKeyColumn("333", PrimaryKeyValue.fromString("111")));
        primaryKeyColumns2.add(new PrimaryKeyColumn("444", PrimaryKeyValue.fromString("222")));
        PrimaryKey primaryKey2 = new PrimaryKey(primaryKeyColumns2);
        s1.setRoutingValues(Arrays.asList(primaryKey1, primaryKey2));
        Assert.assertEquals(s1.getRoutingValues().get(0).getPrimaryKeyColumn(0).getName(), "111");
        Assert.assertEquals(s1.getRoutingValues().get(0).getPrimaryKeyColumn(0).getValue(),
            PrimaryKeyValue.fromString("111"));
        Assert.assertEquals(s1.getRoutingValues().get(0).getPrimaryKeyColumn(1).getName(), "222");
        Assert.assertEquals(s1.getRoutingValues().get(0).getPrimaryKeyColumn(1).getValue(),
            PrimaryKeyValue.fromString("222"));
        Assert.assertEquals(s1.getRoutingValues().get(1).getPrimaryKeyColumn(0).getName(), "333");
        Assert.assertEquals(s1.getRoutingValues().get(1).getPrimaryKeyColumn(0).getValue(),
            PrimaryKeyValue.fromString("111"));
        Assert.assertEquals(s1.getRoutingValues().get(1).getPrimaryKeyColumn(1).getName(), "444");
        Assert.assertEquals(s1.getRoutingValues().get(1).getPrimaryKeyColumn(1).getValue(),
            PrimaryKeyValue.fromString("222"));
    }

    @Test
    public void testGetTimeoutInMillisecond() {
        SearchRequest req = SearchRequest.newBuilder()
                .timeout(12000)
                .build();
        Assert.assertEquals(12000, req.getTimeoutInMillisecond());
    }

    @Test
    public void testGetTimeoutInMillisecondDefault() {
        SearchRequest req = SearchRequest.newBuilder()
                .build();
        Assert.assertEquals(-1, req.getTimeoutInMillisecond());
    }

    @Test
    public void testSetTimeoutInMillisecond() {
        SearchRequest req = new SearchRequest();
        req.setTimeoutInMillisecond(59000);
        Assert.assertEquals(59000, req.getTimeoutInMillisecond());
    }

    /**
     * Method: getOperationName()
     */
    @Test
    public void testGetOperationName() {
        SearchRequest s1 = new SearchRequest();
        Assert.assertEquals(s1.getOperationName(), "Search");
    }

    /**
     * Method: getRequestInfo(boolean prettyFormat)
     */
    @Test
    @Repeat(1)
    public void testGetRequestInfo() {
        SearchRequest searchRequest = SearchRequest.newBuilder()
            .indexName("indexName")
            .tableName("tableName")
            .addColumnsToGet("columnsToGet_f1", "f2", "f3")
            .searchQuery(
                SearchQuery.newBuilder()
                    .query(QueryBuilders.bool()
                        .should(new MatchAllQuery())
                        .filter(new MatchAllQuery())
                        .mustNot(new MatchAllQuery())
                        .must(new MatchAllQuery())
                        .mustNot(QueryBuilders.range("f1").lessThan(9990))
                        .must(QueryBuilders.terms("f2"))
                        .must(QueryBuilders.exists("f3"))
                        .should(QueryBuilders.geoDistance("f4").centerPoint("3,2").distanceInMeter(123.4))
                    )
                    .offset(123)
                    .limit(75)
                    .token("fsfa".getBytes())
                    .addAggregation(AggregationBuilders.min("1", "col_long"))
                    .addAggregation(AggregationBuilders.max("2", "col_long"))
                    .addAggregation(AggregationBuilders.sum("3", "col_long"))
                    .addAggregation(AggregationBuilders.avg("4", "col_long"))
                    .addAggregation(AggregationBuilders.distinctCount("5", "col_long"))
                    .addGroupBy(new GroupByField())
                    .addGroupBy(GroupByBuilders
                        .groupByField("field", "col_keyword")
                        .addSubAggregation(AggregationBuilders.min("1", "col_long"))
                        .addSubAggregation(AggregationBuilders.count("2", "col_long"))
                        .addSubAggregation(AggregationBuilders.sum("3", "col_long"))
                        .addSubAggregation(new SumAggregation())
                        .addSubAggregation(AggregationBuilders.max("4", "col_long"))
                        .addSubAggregation(AggregationBuilders.avg("5", "col_long").missing(13))
                        .addSubGroupBy(new GroupByField())
                        .addSubGroupBy(GroupByBuilders
                            .groupByGeoDistance("123", "col_geo")
                            .origin(12, 23)
                            .addRange(21, 333))
                    )
                    .addGroupBy(GroupByBuilders
                        .groupByGeoDistance("geo", "col_geo")
                        .origin(52.3760, 4.894)
                        .addRange(Double.MIN_VALUE, 100000)
                        .addRange(100000, 300000)
                        .addRange(300000, Double.MAX_VALUE))
                    .addGroupBy(GroupByBuilders
                        .groupByFilter("filter")
                        .addFilter(QueryBuilders.matchAll())
                        .addFilter(QueryBuilders.term("col_keyword", "a"))
                        .addFilter(QueryBuilders.range("col_long").greaterThan(100))
                    )
                    .addGroupBy(GroupByBuilders
                        .groupByRange("range", "col_long")
                        .addRange(0, 100)
                        .addRange(100, 300)
                        .addRange(300, Double.MAX_VALUE)
                    )
                    .build())
            .build();
        searchRequest.printRequestInfo();
        String requestInfo = searchRequest.getRequestInfo(false);
        Assert.assertTrue(requestInfo.contains("queryType"));
        Assert.assertTrue(requestInfo.contains("aggregationType"));
        Assert.assertTrue(requestInfo.contains("groupByType"));
        Assert.assertTrue(requestInfo.contains("GROUP_BY_RANGE"));
        Assert.assertTrue(requestInfo.contains("columnsToGet_f1"));
    }

    @Test
    @Repeat(1)
    public void testPrintRequestInfo() {
        SearchRequest searchRequest1 = SearchRequest.newBuilder()
            .searchQuery(SearchQuery.newBuilder().query(QueryBuilders.matchAll()).build())
            .build();
        SearchRequest searchRequest2 = new SearchRequest();
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(new MatchAllQuery());
        searchRequest2.setSearchQuery(searchQuery);
        Assert.assertEquals(searchRequest1.getRequestInfo(false), searchRequest2.getRequestInfo(false));
        Assert.assertNotSame(searchRequest1.getRequestInfo(true), searchRequest2.getRequestInfo(false));
        searchRequest1.printRequestInfo();
    }
} 
