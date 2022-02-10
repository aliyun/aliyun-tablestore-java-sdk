package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.model.search.query.QueryType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilders;
import com.alicloud.openservices.tablestore.model.search.agg.AvgAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.CountAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.DistinctCountAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.MaxAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.MinAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.SumAggregation;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupBy;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByBuilders;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByField;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByField.Builder;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFilter;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByGeoDistance;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByRange;
import com.alicloud.openservices.tablestore.model.search.groupby.Range;
import com.alicloud.openservices.tablestore.model.search.query.QueryBuilders;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.GeoDistanceSort;
import com.alicloud.openservices.tablestore.model.search.sort.PrimaryKeySort;
import com.alicloud.openservices.tablestore.model.search.sort.ScoreSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort.Sorter;
import org.junit.Assert;
import org.junit.Test;

/**
 * SearchQuery Tester.
 *
 * @author <Authors xunjian>
 * @version 1.0
 * @since <pre>9月 29, 2019</pre>
 */
public class SearchQueryTest extends BaseSearchTest {

    /**
     * Method: getAggregationList() setAggregationList(List<Aggregation> aggregationList)
     */
    @Test
    public void testAggregationList() {
        MaxAggregation a1 = new MaxAggregation();
        a1.setAggName("a1");
        a1.setFieldName("f1");
        a1.setMissing(ColumnValue.fromLong(123));

        AvgAggregation a2 = new AvgAggregation();
        a2.setAggName("a2");
        a2.setFieldName("f2");
        a2.setMissing(ColumnValue.fromLong(333));

        CountAggregation a3 = new CountAggregation();
        a3.setAggName("a3");
        a3.setFieldName("f3");

        MinAggregation a4 = new MinAggregation();
        a4.setAggName("a4");
        a4.setFieldName("f4");
        a4.setMissing(ColumnValue.fromLong(444));

        DistinctCountAggregation a5 = new DistinctCountAggregation();
        a5.setAggName("a5");
        a5.setFieldName("f5");
        a5.setMissing(ColumnValue.fromLong(555));

        SumAggregation a6 = new SumAggregation();
        a6.setAggName("a6");
        a6.setFieldName("f6");
        a6.setMissing(ColumnValue.fromLong(666));

        SearchQuery s1 = new SearchQuery();
        s1.setAggregationList(Arrays.asList(a1, a2, a3, a4, a5, a6));
        SearchRequest build1 = SearchRequest.newBuilder().searchQuery(s1).build();

        SearchQuery s2 = SearchQuery.newBuilder()
            .addAggregation(AggregationBuilders.max("a1", "f1").missing(123))
            .addAggregation(AggregationBuilders.avg("a2", "f2").missing(333))
            .addAggregation(AggregationBuilders.count("a3", "f3"))
            .addAggregation(AggregationBuilders.min("a4", "f4").missing(444))
            .addAggregation(AggregationBuilders.distinctCount("a5", "f5").missing(555))
            .addAggregation(AggregationBuilders.sum("a6", "f6").missing(666))
            .build();
        SearchRequest build2 = SearchRequest.newBuilder().searchQuery(s2).build();

        Assert.assertEquals(build1.getRequestInfo(false), build2.getRequestInfo(false));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));

    }

    /**
     * Method: getGroupByList() setGroupByList(List<GroupBy> groupByList)
     */
    @Test
    public void testGroupByList() {
        GroupByField g1 = new GroupByField();
        g1.setFieldName("g1");
        g1.setGroupByName("n1");
        g1.setSize(10);
        g1.setSubAggregations(Arrays.asList(AggregationBuilders.max("m1", "m1").build(),
            AggregationBuilders.count("m2", "m2").build(),
            AggregationBuilders.count("m3", "m3").build()));

        GroupByGeoDistance g2 = new GroupByGeoDistance();
        g2.setFieldName("g2");
        g2.setGroupByName("n2");
        g2.setOrigin(new GeoPoint(1.3, 5.4333));
        g2.setRanges(Arrays.asList(new Range(111.1, 333.2), new Range(333.3, 222.4)));
        g2.setSubAggregations(Arrays.asList(AggregationBuilders.max("m3", "m5").build(),
            AggregationBuilders.count("m2", "m1").build(),
            AggregationBuilders.count("m1", "m3").build()));
        g2.setSubGroupBys(Collections.<GroupBy>singletonList(g1));

        GroupByRange g3 = new GroupByRange();
        g3.setFieldName("g3");
        g3.setGroupByName("n3");
        g3.setRanges(Arrays.asList(new Range(34.53, 333.2), new Range(3563.3, 42.4)));
        g3.setSubAggregations(Arrays.asList(AggregationBuilders.max("m6", "m5").build(),
            AggregationBuilders.count("m22", "m31").build(),
            AggregationBuilders.count("m51", "m34").build()));

        GroupByFilter g4 = new GroupByFilter();
        g4.setGroupByName("n4");
        g4.setFilters(Arrays.asList(QueryBuilders.range("123").lessThan(123).build(),
            QueryBuilders.match("123", "gdsga").build(),
            QueryBuilders.matchAll().build()));
        g4.setSubAggregations(Arrays.asList(AggregationBuilders.max("m3", "m5").build(),
            AggregationBuilders.count("m2", "m1").build(),
            AggregationBuilders.count("m1", "m3").build()));
        g4.setSubGroupBys(Collections.<GroupBy>singletonList(g1));
        g4.setSubGroupBys(Arrays.asList(g1, g2, g3));

        SearchQuery s1 = new SearchQuery();
        s1.setGroupByList(Arrays.asList(g1, g2, g3, g4));
        SearchRequest build1 = SearchRequest.newBuilder().searchQuery(s1).build();

        Builder builder1 = GroupByBuilders.groupByField("n1", "g1")
            .size(10)
            .addSubAggregation(AggregationBuilders.max("m1", "m1"))
            .addSubAggregation(AggregationBuilders.count("m2", "m2"))
            .addSubAggregation(AggregationBuilders.count("m3", "m3"));

        GroupByGeoDistance.Builder builder2 = GroupByBuilders.groupByGeoDistance("n2", "g2")
            .origin(1.3, 5.4333)
            .addRange(111.1, 333.2)
            .addRange(333.3, 222.4)
            .addSubAggregation(AggregationBuilders.max("m3", "m5"))
            .addSubAggregation(AggregationBuilders.count("m2", "m1"))
            .addSubAggregation(AggregationBuilders.count("m1", "m3"))
            .addSubGroupBy(builder1);

        GroupByRange.Builder builder3 = GroupByBuilders.groupByRange("n3", "g3")
            .addRange(34.53, 333.2)
            .addRange(3563.3, 42.4)
            .addSubAggregation(AggregationBuilders.max("m6", "m5"))
            .addSubAggregation(AggregationBuilders.count("m22", "m31"))
            .addSubAggregation(AggregationBuilders.count("m51", "m34"));

        GroupByFilter.Builder builder4 = GroupByBuilders.groupByFilter("n4")
            .addFilter(QueryBuilders.range("123").lessThan(123))
            .addFilter(QueryBuilders.match("123", "gdsga"))
            .addFilter(QueryBuilders.matchAll())
            .addSubAggregation(AggregationBuilders.max("m3", "m5"))
            .addSubAggregation(AggregationBuilders.count("m2", "m1"))
            .addSubAggregation(AggregationBuilders.count("m1", "m3"))
            .addSubGroupBy(builder1)
            .addSubGroupBy(builder2)
            .addSubGroupBy(builder3);

        SearchQuery s2 = SearchQuery.newBuilder()
            .addGroupBy(builder1)
            .addGroupBy(builder2)
            .addGroupBy(builder3)
            .addGroupBy(builder4)
            .build();
        SearchRequest build2 = SearchRequest.newBuilder().searchQuery(s2).build();
        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));

    }

    /**
     * Method: getOffset() setOffset(Integer offset)
     */
    @Test
    public void testGetOffset() {
        SearchQuery s1 = new SearchQuery();
        s1.setOffset(99);
        SearchRequest build1 = SearchRequest.newBuilder().searchQuery(s1).build();

        SearchQuery s2 = SearchQuery.newBuilder().offset(99).build();
        SearchRequest build2 = SearchRequest.newBuilder().searchQuery(s2).build();

        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));
    }

    /**
     * Method: getLimit() setLimit(Integer limit)
     */
    @Test
    public void testGetSetLimit() {
        SearchQuery s1 = new SearchQuery();
        s1.setLimit(99);
        SearchRequest build1 = SearchRequest.newBuilder().searchQuery(s1).build();

        SearchQuery s2 = SearchQuery.newBuilder().limit(99).build();
        SearchRequest build2 = SearchRequest.newBuilder().searchQuery(s2).build();

        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));
    }

    /**
     * Method: getQuery() setQuery(Query query)
     */
    @Test
    public void testGetAndSetQuery() {
        SearchQuery s1 = new SearchQuery();
        RangeQuery query = new RangeQuery();
        query.setFieldName("f1");
        query.setFrom(ColumnValue.fromLong(123), true);
        s1.setQuery(query);
        SearchRequest build1 = SearchRequest.newBuilder().searchQuery(s1).build();

        SearchQuery s2 = SearchQuery.newBuilder()
            .query(QueryBuilders.range("f1").greaterThanOrEqual(123))
            .build();
        SearchRequest build2 = SearchRequest.newBuilder().searchQuery(s2).build();

        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));
    }

    /**
     * Method: getCollapse() setCollapse(Collapse collapse)
     */
    @Test
    public void testGetAndGetCollapse() {
        SearchQuery s1 = new SearchQuery();
        s1.setCollapse(new Collapse("f1"));
        SearchRequest build1 = SearchRequest.newBuilder().searchQuery(s1).build();

        SearchQuery s2 = SearchQuery.newBuilder().collapse("f1").build();
        SearchRequest build2 = SearchRequest.newBuilder().searchQuery(s2).build();

        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));
    }

    /**
     * Method: getSort() setSort(Sort sort)
     */
    @Test
    public void testGetAndSetSort() {
        List<Sorter> objects = Arrays.<Sorter>asList(
            new FieldSort("f"),
            new ScoreSort(),
            new ScoreSort(),
            new GeoDistanceSort("f3", Arrays.asList("fs", "nfs", "fsf")),
            new PrimaryKeySort());
        Sort sort = new Sort(objects);

        SearchQuery s1 = new SearchQuery();
        s1.setSort(sort);
        SearchRequest searchRequest1 = SearchRequest.newBuilder().searchQuery(s1).build();

        SearchQuery searchQuery2 = SearchQuery.newBuilder().sort(sort).build();
        SearchRequest searchRequest2 = new SearchRequest();
        searchRequest2.setSearchQuery(searchQuery2);

        Assert.assertEquals(searchRequest1.getRequestInfo(true), searchRequest2.getRequestInfo(true));
        Assert.assertNotSame(searchRequest1.getRequestInfo(true), searchRequest2.getRequestInfo(false));
    }

    /**
     * Method: isGetTotalCount() setGetTotalCount(boolean getTotalCount)
     */
    @Test
    public void testTotalCount() {
        SearchQuery s1 = new SearchQuery();
        s1.setGetTotalCount(true);
        SearchRequest build1 = SearchRequest.newBuilder().searchQuery(s1).build();

        SearchQuery s2 = SearchQuery.newBuilder().getTotalCount(true).build();
        SearchRequest build2 = SearchRequest.newBuilder().searchQuery(s2).build();

        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));
    }

    /**
     * Method: getToken()  setToken(byte[] token)
     */
    @Test
    public void testGetAndSetToken() {
        SearchQuery s1 = new SearchQuery();
        s1.setToken("fsfsf".getBytes());
        SearchRequest build1 = SearchRequest.newBuilder().searchQuery(s1).build();

        SearchQuery s2 = SearchQuery.newBuilder().token("fsfsf".getBytes()).build();
        SearchRequest build2 = SearchRequest.newBuilder().searchQuery(s2).build();

        Assert.assertEquals(build1.getRequestInfo(true), build2.getRequestInfo(true));
        Assert.assertNotSame(build1.getRequestInfo(true), build2.getRequestInfo(false));
    }

    @Test
    public void testQueryType() {
        Assert.assertEquals(QueryType.QueryType_MatchQuery, QueryBuilders.match("","").build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_MatchPhraseQuery, QueryBuilders.matchPhrase("","").build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_TermQuery, QueryBuilders.term("","").build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_RangeQuery, QueryBuilders.range("").build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_PrefixQuery, QueryBuilders.prefix("","").build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_BoolQuery, QueryBuilders.bool().build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_ConstScoreQuery, QueryBuilders.constScore().build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_FunctionScoreQuery, QueryBuilders.functionScore("").build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_NestedQuery, QueryBuilders.nested().build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_WildcardQuery, QueryBuilders.wildcard("", "").build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_MatchAllQuery, QueryBuilders.matchAll().build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_GeoBoundingBoxQuery, QueryBuilders.geoBoundingBox("").build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_GeoDistanceQuery, QueryBuilders.geoDistance("").build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_GeoPolygonQuery, QueryBuilders.geoPolygon("").build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_TermsQuery, QueryBuilders.terms("").build().getQueryType());
        Assert.assertEquals(QueryType.QueryType_ExistsQuery, QueryBuilders.exists("").build().getQueryType());
    }

    @Test
    public void testQueryJsonField() {
        Gson gson = new GsonBuilder()
            .disableHtmlEscaping()
            .disableInnerClassSerialization()
            .serializeNulls()
            .serializeSpecialFloatingPointValues()
            .enableComplexMapKeySerialization().create();
        Assert.assertTrue(gson.toJson(QueryBuilders.match("", "").build()).contains("queryType"));
        Assert.assertTrue(gson.toJson(QueryBuilders.match("", "").build()).contains("QueryType_MatchQuery"));


        Assert.assertTrue(gson.toJson(QueryBuilders.wildcard("", "").build()).contains("queryType"));
        Assert.assertTrue(gson.toJson(QueryBuilders.wildcard("", "").build()).contains("QueryType_WildcardQuery"));

        Assert.assertTrue(gson.toJson(QueryBuilders.geoDistance("").build()).contains("queryType"));
        Assert.assertTrue(gson.toJson(QueryBuilders.geoDistance("").build()).contains("QueryType_GeoDistanceQuery"));
    }
} 
