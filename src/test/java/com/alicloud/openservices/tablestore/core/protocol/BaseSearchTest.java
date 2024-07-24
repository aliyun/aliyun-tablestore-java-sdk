package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.utils.Repeat;
import com.alicloud.openservices.tablestore.core.utils.RepeatRule;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.search.Collapse;
import com.alicloud.openservices.tablestore.model.search.DateTimeUnit;
import com.alicloud.openservices.tablestore.model.search.DateTimeValue;
import com.alicloud.openservices.tablestore.model.search.GeoHashPrecision;
import com.alicloud.openservices.tablestore.model.search.GeoPoint;
import com.alicloud.openservices.tablestore.model.search.ParallelScanRequest;
import com.alicloud.openservices.tablestore.model.search.ScanQuery;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchRequest.ColumnsToGet;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationType;
import com.alicloud.openservices.tablestore.model.search.agg.AvgAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.CountAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.DistinctCountAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.MaxAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.MinAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.PercentilesAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.SumAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.TopRowsAggregation;
import com.alicloud.openservices.tablestore.model.search.groupby.FieldRange;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupBy;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByDateHistogram;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByField;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFilter;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByGeoDistance;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByGeoGrid;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByHistogram;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByRange;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByType;
import com.alicloud.openservices.tablestore.model.search.groupby.Range;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByComposite;
import com.alicloud.openservices.tablestore.model.search.highlight.Highlight;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightEncoder;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightFragmentOrder;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightParameter;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.ConstScoreQuery;
import com.alicloud.openservices.tablestore.model.search.query.DecayFuncDateParam;
import com.alicloud.openservices.tablestore.model.search.query.DecayFuncGeoParam;
import com.alicloud.openservices.tablestore.model.search.query.DecayFuncNumericParam;
import com.alicloud.openservices.tablestore.model.search.query.DecayFunction;
import com.alicloud.openservices.tablestore.model.search.query.DecayParam;
import com.alicloud.openservices.tablestore.model.search.query.ExistsQuery;
import com.alicloud.openservices.tablestore.model.search.query.FieldValueFactor;
import com.alicloud.openservices.tablestore.model.search.query.FieldValueFactorFunction;
import com.alicloud.openservices.tablestore.model.search.query.FunctionScoreQuery;
import com.alicloud.openservices.tablestore.model.search.query.FunctionsScoreQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoBoundingBoxQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoDistanceQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoPolygonQuery;
import com.alicloud.openservices.tablestore.model.search.query.KnnVectorQuery;
import com.alicloud.openservices.tablestore.model.search.query.InnerHits;
import com.alicloud.openservices.tablestore.model.search.query.MatchAllQuery;
import com.alicloud.openservices.tablestore.model.search.query.MatchPhraseQuery;
import com.alicloud.openservices.tablestore.model.search.query.MatchQuery;
import com.alicloud.openservices.tablestore.model.search.query.MultiValueMode;
import com.alicloud.openservices.tablestore.model.search.query.NestedQuery;
import com.alicloud.openservices.tablestore.model.search.query.PrefixQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.QueryOperator;
import com.alicloud.openservices.tablestore.model.search.query.QueryType;
import com.alicloud.openservices.tablestore.model.search.query.RandomFunction;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;
import com.alicloud.openservices.tablestore.model.search.query.ScoreFunction;
import com.alicloud.openservices.tablestore.model.search.query.ScoreMode;
import com.alicloud.openservices.tablestore.model.search.query.SuffixQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermsQuery;
import com.alicloud.openservices.tablestore.model.search.query.WildcardQuery;
import com.alicloud.openservices.tablestore.model.search.sort.DocSort;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.GeoDistanceSort;
import com.alicloud.openservices.tablestore.model.search.sort.GeoDistanceType;
import com.alicloud.openservices.tablestore.model.search.sort.GroupBySorter;
import com.alicloud.openservices.tablestore.model.search.sort.GroupKeySort;
import com.alicloud.openservices.tablestore.model.search.sort.NestedFilter;
import com.alicloud.openservices.tablestore.model.search.sort.PrimaryKeySort;
import com.alicloud.openservices.tablestore.model.search.sort.RowCountSort;
import com.alicloud.openservices.tablestore.model.search.sort.ScoreSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort.Sorter;
import com.alicloud.openservices.tablestore.model.search.sort.SortMode;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import com.alicloud.openservices.tablestore.model.search.sort.SubAggSort;
import com.alicloud.openservices.tablestore.model.search.vector.VectorDataType;
import com.alicloud.openservices.tablestore.model.search.vector.VectorMetricType;
import com.alicloud.openservices.tablestore.model.search.vector.VectorOptions;
import com.google.common.base.Supplier;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base test for SearchIndex. Using this base test can avoid some wrong implementation in protocol serialization and deserialization.
 */
public abstract class BaseSearchTest {

    protected static Logger logger = LoggerFactory.getLogger(BaseSearchTest.class);

    /**
     * repeat many times to avoid random fail.
     */
    @Rule
    public RepeatRule repeatRule = new RepeatRule(500);

    private static final Gson GSON = new GsonBuilder()
            .disableHtmlEscaping()
            .serializeNulls()
            .serializeSpecialFloatingPointValues()
            .enableComplexMapKeySerialization()
            .setExclusionStrategies(new ExclusionStrategy() {
                @Override
                public boolean shouldSkipField(FieldAttributes f) {
                    return "rawData".equals(f.getName())
                            && "[B".equals(f.getDeclaredClass().getName())
                            && f.getDeclaringClass().equals(PrimaryKeyValue.class);
                }
                @Override
                public boolean shouldSkipClass(Class<?> aClass) {
                    return false;
                }
            })
            .create();

    private static final Random RANDOM = new Random();
    private static final char[] ALPHABET = "0123456789_abcdefghijklmnopqrstuvwxyz".toCharArray();

    protected Random random() {
        return RANDOM;
    }

    @Test
    public void testAllQueryShouldBeTest() {
        // 'QueryType_None' is useless
        int actualQueryTypeKinds = QueryType.values().length - 1;
        assertEquals("all Query should be test. if you add a new kind, please add it to here", actualQueryTypeKinds, getAllQuerySupplier().size());
        assertEquals(actualQueryTypeKinds, Search.QueryType.values().length);
    }

    @Test
    public void testAllGroupByShouldBeTest() {
        assertEquals("all GroupBy should be test. if you add a new kind, please add it to here", GroupByType.values().length, getAllGroupBySupplier().size());
        assertEquals(GroupByType.values().length, Search.GroupByType.values().length);
    }

    @Test
    public void testAllAggregationShouldBeTest() {
        assertEquals("all Aggregation should be test. if you add a new kind, please add it to here", AggregationType.values().length, getAllAggregationSupplier().size());
        assertEquals(AggregationType.values().length, Search.AggregationType.values().length);
    }

    @Test
    public void testAllSorterShouldBeTest() {
        assertEquals("all Sort.Sorter should be test. if you add a new kind, please add it to here", 5, getAllSortSorterSupplier().size());
    }

    @Test
    @Repeat(value = 100)
    public void testAllFieldShouldBeTest() {
        randomSearchQuery();
        randomSearchRequest();
        randomScanQuery();
        randomParallelScanRequest();
        randomFrom(getAllQuerySupplier()).get();
        randomFrom(getAllGroupBySupplier()).get();
        randomFrom(getAllAggregationSupplier()).get();
        randomFrom(getAllSortSorterSupplier()).get();
    }

    /**
     * 保证所有的字段都被测试到。防止后面同学修改代码漏掉一些实现或者漏掉测试。
     * 假如一个类有新的成员变量加入，case一定通不过。一个类新加成员变量后，这时候需要
     * 1. 修改下面的randomXXX的实现，将新的成员变量也加入到random实现中。
     * 2. 修改'maxField'才能通过。
     * 3. 验证相关的反序列化case(TestSearchProtocolParser等)可以通过。
     */
    public static void assertAllFieldTested(Object o, int maxField) {
        JsonObject jsonObject = GSON.toJsonTree(o).getAsJsonObject();
        assertTrue("class[" + o.getClass().getSimpleName() + "] except " + maxField + " fields but get " + jsonObject.entrySet().size() + " fields. Maybe add a new field.",
                jsonObject.entrySet().size() <= maxField);
    }

    public static void assertJsonEquals(Object origin, Object newParsed) {
        assertEquals(GSON.toJson(origin), GSON.toJson(newParsed));
    }

    public static String randomString(int length) {
        char[] ret = new char[length];
        for (int i = 0; i < length; i++) {
            ret[i] = ALPHABET[(int) (System.nanoTime() % ALPHABET.length)];
        }
        return new String(ret);
    }

    public static SortOrder randomSortOrder() {
        return randomFrom(SortOrder.values());
    }

    public static GroupKeySort randomGroupKeySort() {
        GroupKeySort groupKeySort = new GroupKeySort();
        groupKeySort.setOrder(randomSortOrder());
        assertAllFieldTested(groupKeySort, 1);
        return groupKeySort;
    }


    public static RowCountSort randomRowCountSort() {
        RowCountSort sort = new RowCountSort();
        sort.setOrder(randomSortOrder());
        assertAllFieldTested(sort, 1);
        return sort;
    }

    public static SubAggSort randomSubAggSort() {
        SubAggSort sort = new SubAggSort();
        sort.setOrder(randomSortOrder());
        sort.setSubAggName(randomString(10));
        assertAllFieldTested(sort, 2);
        return sort;
    }

    public static GroupBySorter randomGroupBySorter() {
        GroupBySorter sorter = new GroupBySorter();
        int i = RANDOM.nextInt(9);
        if (i < 3) {
            sorter.setSubAggSort(randomSubAggSort());
        } else if (i < 6) {
            sorter.setGroupKeySort(randomGroupKeySort());
        } else {
            sorter.setRowCountSort(randomRowCountSort());
        }
        assertAllFieldTested(sorter, 3);
        return sorter;
    }

    public static List<GroupBySorter> randomGroupBySorterList() {
        List<GroupBySorter> list = new ArrayList<GroupBySorter>();
        int size = RANDOM.nextInt(4);
        for (int i = 0; i < size; i++) {
            list.add(randomGroupBySorter());
        }
        return list;
    }

    public static FieldSort randomFieldSort() {
        FieldSort sort = new FieldSort(randomString(10), randomSortOrder());
        sort.setMode(randomSortMode());
        if (RANDOM.nextBoolean()) {
            sort.setNestedFilter(randomNestedFilter());
        }
        sort.setOrder(randomSortOrder());
        assertAllFieldTested(sort, 7);
        if (RANDOM.nextBoolean()) {
            sort.setMissing(randomColumnValue());
        }
        if (RANDOM.nextBoolean()) {
            sort.setMissingValue(randomColumnValue());
        }
        if (RANDOM.nextBoolean()) {
            sort.setMissingField(randomString(10));
        }
        return sort;
    }

    public static ScoreSort randomScoreSort() {
        ScoreSort sort = new ScoreSort();
        sort.setOrder(randomSortOrder());
        assertAllFieldTested(sort, 1);
        return sort;
    }

    private static NestedFilter randomNestedFilter() {
        NestedFilter nestedFilter = new NestedFilter(randomString(4), randomQuery());
        assertAllFieldTested(nestedFilter, 2);
        return nestedFilter;
    }

    public static PrimaryKeySort randomPrimaryKeySort() {
        PrimaryKeySort sort = new PrimaryKeySort();
        sort.setOrder(randomSortOrder());
        assertAllFieldTested(sort, 1);
        return sort;
    }

    public static DocSort randomDocSort() {
        DocSort sort = new DocSort();
        sort.setOrder(randomSortOrder());
        assertAllFieldTested(sort, 1);
        return sort;
    }

    public static SortMode randomSortMode() {
        return randomFrom(SortMode.values());
    }

    public static GeoDistanceSort randomGeoDistanceSort() {
        List<String> points = new ArrayList<String>();
        int size = RANDOM.nextInt(3);
        for (int i = 0; i < size; i++) {
            points.add(randomString(3));
        }
        GeoDistanceSort sort = new GeoDistanceSort(randomString(10), points);
        sort.setMode(randomSortMode());
        sort.setOrder(randomSortOrder());
        if (RANDOM.nextBoolean()) {
            sort.setDistanceType(RANDOM.nextBoolean() ? GeoDistanceType.ARC : GeoDistanceType.PLANE);
        }
        if (RANDOM.nextBoolean()) {
            sort.setNestedFilter(randomNestedFilter());
        }
        assertAllFieldTested(sort, 6);
        return sort;
    }

    public static Sort.Sorter randomSorter() {
        return randomFrom(getAllSortSorterSupplier()).get();
    }

    public static List<Supplier<Sort.Sorter>> getAllSortSorterSupplier() {
        List<Supplier<Sort.Sorter>> all = new ArrayList<Supplier<Sorter>>();
        all.add(new Supplier<Sorter>() {
            @Override
            public Sorter get() {
                return randomFieldSort();
            }
        });
        all.add(new Supplier<Sorter>() {
            @Override
            public Sorter get() {
                return randomGeoDistanceSort();
            }
        });
        all.add(new Supplier<Sorter>() {
            @Override
            public Sorter get() {
                return randomScoreSort();
            }
        });
        all.add(new Supplier<Sorter>() {
            @Override
            public Sorter get() {
                return randomPrimaryKeySort();
            }
        });
        all.add(new Supplier<Sorter>() {
            @Override
            public Sorter get() {
                return randomDocSort();
            }
        });
        return all;
    }

    public static Sort randomSort() {
        List<Sort.Sorter> sorters = new ArrayList<Sorter>();
        int size = RANDOM.nextInt(3);
        for (int i = 0; i < size; i++) {
            sorters.add(randomSorter());
        }
        if (RANDOM.nextBoolean()) {
            return new Sort(sorters, RANDOM.nextBoolean());
        }

        return new Sort(sorters);
    }

    public static Query randomQuery() {
        return randomFrom(getAllQuerySupplier()).get();
    }

    public static Highlight randomHighlight() {
        Highlight.Builder highlightBuilder = Highlight.newBuilder();
        switch (RANDOM.nextInt(3)) {
            case 0:
                highlightBuilder.highlightEncoder(HighlightEncoder.PLAIN);
                break;
            case 1:
                highlightBuilder.highlightEncoder(HighlightEncoder.HTML);
                break;
            default:
                break;
        }

        for (int i = 0; i < RANDOM.nextInt(10); i++) {
            String fieldName = null;
            if (RANDOM.nextBoolean()) {
                fieldName = randomString(10);
            }

            HighlightParameter highlightParameter = new HighlightParameter();
            switch (RANDOM.nextInt(3)) {
                case 0:
                    highlightParameter.setHighlightFragmentOrder(HighlightFragmentOrder.TEXT_SEQUENCE);
                    break;
                case 1:
                    highlightParameter.setHighlightFragmentOrder(HighlightFragmentOrder.SCORE);
                    break;
                default:
                    break;

            }

            if (RANDOM.nextBoolean()) {
                highlightParameter.setPreTag(randomString(16));
                highlightParameter.setPostTag(randomString(16));
            }

            if (RANDOM.nextBoolean()) {
                highlightParameter.setNumberOfFragments(RANDOM.nextInt(10));
            }

            if (RANDOM.nextBoolean()) {
                highlightParameter.setFragmentSize(RANDOM.nextInt(100));
            }
            highlightBuilder.addFieldHighlightParam(fieldName, highlightParameter);
        }
        return highlightBuilder.build();
    }

    public static InnerHits randomInnerHits() {
        InnerHits.Builder innerHitsBuilder = InnerHits.newBuilder();

        if (RANDOM.nextBoolean()) {
            innerHitsBuilder.sort(randomSort());
        }

        if (RANDOM.nextBoolean()) {
            innerHitsBuilder.offset(RANDOM.nextInt());
        }

        if (RANDOM.nextBoolean()) {
            innerHitsBuilder.limit(RANDOM.nextInt());
        }

        if (RANDOM.nextBoolean()) {
            innerHitsBuilder.highlight(randomHighlight());
        }

        return innerHitsBuilder.build();
    }

    public static List<Supplier<Query>> getAllQuerySupplier() {
        List<Supplier<Query>> all = new ArrayList<Supplier<Query>>();
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomWildcardQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomTermsQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomTermQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomRangeQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomPrefixQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomNestedQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomMatchQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomMatchPhraseQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomMatchAllQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomGeoPolygonQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomGeoBoundingBoxQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomGeoDistanceQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomFunctionScoreQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomFunctionsScoreQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomExistsQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomConstScoreQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomKnnVectorQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomBoolQuery();
            }
        });
        all.add(new Supplier<Query>() {
            @Override
            public Query get() {
                return randomSuffixQuery();
            }
        });
        return all;
    }

    public static List<Query> randomQueries() {
        List<Query> queryList = new ArrayList<Query>();
        int i = RANDOM.nextInt(4);
        for (int i1 = 0; i1 < i; i1++) {
            queryList.add(randomQuery());
        }
        return queryList;
    }

    public static WildcardQuery randomWildcardQuery() {
        WildcardQuery query = new WildcardQuery();
        query.setFieldName(randomString(10));
        query.setValue(randomString(20));
        if (RANDOM.nextBoolean()) {
            query.setWeight(RANDOM.nextFloat());
        }
        assertAllFieldTested(query, 4);
        return query;
    }

    public static TermsQuery randomTermsQuery() {
        TermsQuery query = new TermsQuery();
        query.setFieldName(randomString(10));
        List<ColumnValue> terms = new ArrayList<ColumnValue>();
        int size = RANDOM.nextInt(4);
        for (int i = 0; i < size; i++) {
            terms.add(ValueUtil.toColumnValue(randomString(20)));
        }
        query.setTerms(terms);
        if (RANDOM.nextBoolean()) {
            query.setWeight(RANDOM.nextFloat());
        }
        assertAllFieldTested(query, 4);
        return query;
    }

    public static TermQuery randomTermQuery() {
        TermQuery query = new TermQuery();
        query.setFieldName(randomString(10));
        query.setTerm(ValueUtil.toColumnValue(randomString(20)));
        if (RANDOM.nextBoolean()) {
            query.setWeight(RANDOM.nextFloat());
        }
        assertAllFieldTested(query, 4);
        return query;
    }

    public static RangeQuery randomRangeQuery() {
        RangeQuery query = new RangeQuery();
        query.setFieldName(randomString(10));
        query.setFrom(ValueUtil.toColumnValue(randomString(20)));
        query.setTo(ValueUtil.toColumnValue(randomString(20)));
        query.setIncludeUpper(RANDOM.nextBoolean());
        query.setIncludeLower(RANDOM.nextBoolean());
        assertAllFieldTested(query, 6);
        return query;
    }

    public static PrefixQuery randomPrefixQuery() {
        PrefixQuery query = new PrefixQuery();
        query.setFieldName(randomString(10));
        query.setPrefix(randomString(20));
        if (RANDOM.nextBoolean()) {
            query.setWeight(RANDOM.nextFloat());
        }
        assertAllFieldTested(query, 4);
        return query;
    }

    public static <T> T randomFrom(List<T> values) {
        int total = values.size();
        return values.get(RANDOM.nextInt(total));
    }

    public static <T> T randomFrom(T[] values) {
        int total = values.length;
        return values[RANDOM.nextInt(total)];
    }

    public static <T> T randomFromExcludingUnknown(T[] values) {
        List<T> filteredValues = new ArrayList<T>();
        for (T value : values) {
            if (!value.toString().equals("UNKNOWN")) {
                filteredValues.add(value);
            }
        }
        int total = filteredValues.size();
        if (total == 0) {
            throw new IllegalArgumentException("the values do not contain other type except UNKNOWN");
        }
        return filteredValues.get(RANDOM.nextInt(total));
    }

    public static <T> List<T> randomList(Supplier<T> supplier) {
        int total = RANDOM.nextInt(5);
        List<T> list = new ArrayList<T>();
        for (int i = 0; i < total; i++) {
            list.add(supplier.get());
        }
        return list;
    }

    public static ScoreMode randomScoreMode() {
        return randomFrom(ScoreMode.values());
    }

    public static QueryOperator randomQueryOperator() {
        return randomFrom(QueryOperator.values());
    }

    public static NestedQuery randomNestedQuery() {
        NestedQuery query = new NestedQuery();
        query.setPath(randomString(10));
        query.setQuery(randomQuery());
        if (RANDOM.nextBoolean()) {
            query.setWeight(RANDOM.nextFloat());
        }
        query.setScoreMode(randomScoreMode());
        assertAllFieldTested(query, 6);
        return query;
    }

    public static MatchQuery randomMatchQuery() {
        MatchQuery query = new MatchQuery();
        query.setFieldName(randomString(10));
        query.setText(randomString(20));
        if (RANDOM.nextBoolean()) {
            query.setWeight(RANDOM.nextFloat());
        }
        if (RANDOM.nextBoolean()) {
            query.setMinimumShouldMatch(RANDOM.nextInt());
        }
        if (RANDOM.nextBoolean()) {
            query.setOperator(randomQueryOperator());
        }
        assertAllFieldTested(query, 6);
        return query;
    }

    public static MatchAllQuery randomMatchAllQuery() {
        MatchAllQuery matchAllQuery = new MatchAllQuery();
        assertAllFieldTested(matchAllQuery, 1);
        return matchAllQuery;
    }

    public static MatchPhraseQuery randomMatchPhraseQuery() {
        MatchPhraseQuery query = new MatchPhraseQuery();
        query.setFieldName(randomString(10));
        query.setText(randomString(20));
        if (RANDOM.nextBoolean()) {
            query.setWeight(RANDOM.nextFloat());
        }
        assertAllFieldTested(query, 4);
        return query;
    }

    public static GeoPolygonQuery randomGeoPolygonQuery() {
        GeoPolygonQuery query = new GeoPolygonQuery();
        query.setFieldName(randomString(10));
        int size = RANDOM.nextInt(3);
        List<String> points = new ArrayList<String>();
        for (int i = 0; i < size; i++) {
            points.add(randomString(10));
        }
        query.setPoints(points);
        assertAllFieldTested(query, 3);
        return query;
    }

    public static GeoDistanceQuery randomGeoDistanceQuery() {
        GeoDistanceQuery query = new GeoDistanceQuery();
        query.setFieldName(randomString(10));
        query.setCenterPoint(randomString(10));
        query.setDistanceInMeter(RANDOM.nextDouble());
        assertAllFieldTested(query, 4);
        return query;
    }

    public static GeoBoundingBoxQuery randomGeoBoundingBoxQuery() {
        GeoBoundingBoxQuery query = new GeoBoundingBoxQuery();
        query.setFieldName(randomString(10));
        query.setTopLeft(randomString(10));
        query.setBottomRight(randomString(10));
        assertAllFieldTested(query, 4);
        return query;
    }

    public static FunctionScoreQuery randomFunctionScoreQuery() {
        FunctionScoreQuery query = new FunctionScoreQuery(randomQuery(), new FieldValueFactor(randomString(10)));
        assertAllFieldTested(query, 3);
        return query;
    }

    private static List<ScoreFunction> randomScoreFunctions() {
        List<ScoreFunction> scoreFunctions = new ArrayList<ScoreFunction>();
        int length = RANDOM.nextInt(3);
        for (int i = 0; i <= length; i++) {
            int randomFunctionType = RANDOM.nextInt(4);
            switch (randomFunctionType) {
                case 0:
                    scoreFunctions.add(ScoreFunction.newBuilder().weight(RANDOM.nextFloat()).filter(randomQuery()).build());
                    break;
                case 1:
                    scoreFunctions.add(ScoreFunction.newBuilder().weight(RANDOM.nextFloat()).filter(randomQuery())
                            .randomFunction(RandomFunction.newBuilder().build()).build());
                    break;
                case 2:
                    scoreFunctions.add(ScoreFunction.newBuilder().weight(RANDOM.nextFloat()).filter(randomQuery())
                            .fieldValueFactorFunction(FieldValueFactorFunction.newBuilder()
                                    .factor(RANDOM.nextFloat() + 0.001f)
                                    .missing(RANDOM.nextDouble())
                                    .fieldName(randomString(10))
                                    .modifier(randomFromExcludingUnknown(FieldValueFactorFunction.FunctionModifier.values())).build()).build());
                    break;
                default:
                    int randomParamType = RANDOM.nextInt(3);
                    DecayParam decayParam;
                    switch (randomParamType) {
                        case 0:
                            decayParam = DecayFuncGeoParam.newBuilder().scale(RANDOM.nextDouble() + 0.001).origin(randomString(5)).offset(RANDOM.nextDouble() + 0.001).build();
                            break;
                        case 1:
                            decayParam = DecayFuncDateParam.newBuilder().scale(randomDateTimeValue()).originString(randomString(5)).offset(randomDateTimeValue()).build();
                            break;
                        default:
                            decayParam = DecayFuncNumericParam.newBuilder().scale(RANDOM.nextDouble() + 0.001).origin(RANDOM.nextDouble() + 0.001).offset(RANDOM.nextDouble() + 0.001).build();
                    }
                    scoreFunctions.add(ScoreFunction.newBuilder().weight(RANDOM.nextFloat()).filter(randomQuery())
                            .decayFunction(DecayFunction.newBuilder()
                                    .fieldName(randomString(10))
                                    .decay(RANDOM.nextDouble() + 0.001)
                                    .mathFunction(randomFromExcludingUnknown(DecayFunction.MathFunction.values()))
                                    .multiValueMode(randomFromExcludingUnknown(MultiValueMode.values()))
                                    .decayParam(decayParam).build()).build());
            }
        }
        return scoreFunctions;
    }

    private static FunctionsScoreQuery randomFunctionsScoreQuery() {
        FunctionsScoreQuery query = new FunctionsScoreQuery();
        query.setQuery(randomQuery());
        query.setFunctions(randomScoreFunctions());
        query.setScoreMode(randomFromExcludingUnknown(FunctionsScoreQuery.ScoreMode.values()));
        query.setCombineMode(randomFromExcludingUnknown(FunctionsScoreQuery.CombineMode.values()));
        query.setMinScore(RANDOM.nextFloat());
        query.setMaxScore(RANDOM.nextFloat());
        assertAllFieldTested(query, 7);
        return query;
    }

    public static ExistsQuery randomExistsQuery() {
        ExistsQuery query = new ExistsQuery();
        query.setFieldName(randomString(10));
        assertAllFieldTested(query, 2);
        return query;
    }

    public static ConstScoreQuery randomConstScoreQuery() {
        ConstScoreQuery query = new ConstScoreQuery();
        query.setFilter(randomQuery());
        assertAllFieldTested(query, 2);
        return query;
    }

    public static KnnVectorQuery randomKnnVectorQuery() {
        KnnVectorQuery query = new KnnVectorQuery();
        query.setFieldName(randomString(3));
        query.setTopK(RANDOM.nextInt());
        float[] floats = new float[RANDOM.nextInt(100) + 1];
        for (int i = 0; i < floats.length; i++) {
            floats[i] = RANDOM.nextFloat();
        }
        query.setFloat32QueryVector(floats);
        byte[] bytes = new byte[RANDOM.nextInt(100) + 1];
        RANDOM.nextBytes(bytes);
        query.setFilter(randomQuery());
        query.setWeight(RANDOM.nextFloat());
        assertAllFieldTested(query, 6);
        return query;
    }

    public static BoolQuery randomBoolQuery() {
        BoolQuery query = new BoolQuery();
        if (RANDOM.nextBoolean()) {
            query.setMinimumShouldMatch(RANDOM.nextInt());
        }
        query.setFilterQueries(randomQueries());
        query.setShouldQueries(randomQueries());
        query.setMustQueries(randomQueries());
        query.setMustNotQueries(randomQueries());
        assertAllFieldTested(query, 6);
        return query;
    }

    public static SuffixQuery randomSuffixQuery() {
        SuffixQuery query = new SuffixQuery();
        query.setFieldName(randomString(10));
        query.setSuffix(randomString(20));
        if (RANDOM.nextBoolean()) {
            query.setWeight(RANDOM.nextFloat());
        }
        assertAllFieldTested(query, 4);
        return query;
    }

    public static MaxAggregation randomMaxAggregation() {
        MaxAggregation aggregation = new MaxAggregation();
        aggregation.setAggName(randomString(10));
        aggregation.setFieldName(randomString(10));
        if (RANDOM.nextBoolean()) {
            aggregation.setMissing(ValueUtil.toColumnValue(RANDOM.nextInt()));
        }
        assertAllFieldTested(aggregation, 4);
        return aggregation;
    }

    public static SumAggregation randomSumAggregation() {
        SumAggregation aggregation = new SumAggregation();
        aggregation.setAggName(randomString(10));
        aggregation.setFieldName(randomString(10));
        if (RANDOM.nextBoolean()) {
            aggregation.setMissing(ValueUtil.toColumnValue(RANDOM.nextInt()));
        }
        assertAllFieldTested(aggregation, 4);
        return aggregation;
    }

    public static MinAggregation randomMinAggregation() {
        MinAggregation aggregation = new MinAggregation();
        aggregation.setAggName(randomString(10));
        aggregation.setFieldName(randomString(10));
        if (RANDOM.nextBoolean()) {
            aggregation.setMissing(ValueUtil.toColumnValue(RANDOM.nextInt()));
        }
        assertAllFieldTested(aggregation, 4);
        return aggregation;
    }

    public static AvgAggregation randomAvgAggregation() {
        AvgAggregation aggregation = new AvgAggregation();
        aggregation.setAggName(randomString(10));
        aggregation.setFieldName(randomString(10));
        if (RANDOM.nextBoolean()) {
            aggregation.setMissing(ValueUtil.toColumnValue(RANDOM.nextInt()));
        }
        assertAllFieldTested(aggregation, 4);
        return aggregation;
    }

    public static CountAggregation randomCountAggregation() {
        CountAggregation aggregation = new CountAggregation();
        aggregation.setAggName(randomString(10));
        aggregation.setFieldName(randomString(10));
        assertAllFieldTested(aggregation, 3);
        return aggregation;
    }

    public static DistinctCountAggregation randomDistinctCountAggregation() {
        DistinctCountAggregation aggregation = new DistinctCountAggregation();
        aggregation.setAggName(randomString(10));
        aggregation.setFieldName(randomString(10));
        if (RANDOM.nextBoolean()) {
            aggregation.setMissing(ValueUtil.toColumnValue(RANDOM.nextInt()));
        }
        assertAllFieldTested(aggregation, 4);
        return aggregation;
    }

    public static PercentilesAggregation randomPercentilesAggregation() {
        PercentilesAggregation aggregation = new PercentilesAggregation();
        aggregation.setAggName(randomString(10));
        aggregation.setFieldName(randomString(10));
        if (RANDOM.nextBoolean()) {
            aggregation.setMissing(ValueUtil.toColumnValue(RANDOM.nextInt()));
        }
        int size = RANDOM.nextInt(4);
        List<Double> percentiles = new ArrayList<Double>();
        for (int i = 0; i < size; i++) {
            percentiles.add(RANDOM.nextDouble());
        }
        aggregation.setPercentiles(percentiles);
        assertAllFieldTested(aggregation, 5);
        return aggregation;
    }

    public static TopRowsAggregation randomTopRowsAggregation() {
        TopRowsAggregation aggregation = new TopRowsAggregation();
        aggregation.setAggName(randomString(10));
        if (RANDOM.nextBoolean()) {
            aggregation.setLimit(RANDOM.nextInt());
        }
        if (RANDOM.nextBoolean()) {
            aggregation.setSort(randomSort());
        }
        assertAllFieldTested(aggregation, 4);
        return aggregation;
    }

    public static List<Supplier<Aggregation>> getAllAggregationSupplier() {
        List<Supplier<Aggregation>> all = new ArrayList<Supplier<Aggregation>>();
        all.add(new Supplier<Aggregation>() {
            @Override
            public Aggregation get() {
                return randomAvgAggregation();
            }
        });
        all.add(new Supplier<Aggregation>() {
            @Override
            public Aggregation get() {
                return randomCountAggregation();
            }
        });
        all.add(new Supplier<Aggregation>() {
            @Override
            public Aggregation get() {
                return randomMaxAggregation();
            }
        });
        all.add(new Supplier<Aggregation>() {
            @Override
            public Aggregation get() {
                return randomMinAggregation();
            }
        });
        all.add(new Supplier<Aggregation>() {
            @Override
            public Aggregation get() {
                return randomSumAggregation();
            }
        });
        all.add(new Supplier<Aggregation>() {
            @Override
            public Aggregation get() {
                return randomDistinctCountAggregation();
            }
        });
        all.add(new Supplier<Aggregation>() {
            @Override
            public Aggregation get() {
                return randomPercentilesAggregation();
            }
        });
        all.add(new Supplier<Aggregation>() {
            @Override
            public Aggregation get() {
                return randomTopRowsAggregation();
            }
        });

        return all;
    }

    public static Aggregation randomAggregation() {
        return randomFrom(getAllAggregationSupplier()).get();
    }

    public static List<Aggregation> randomAggregations() {
        List<Aggregation> aggregations = new ArrayList<Aggregation>();
        int size = RANDOM.nextInt(4);
        for (int i = 0; i < size; i++) {
            aggregations.add(randomAggregation());
        }
        return aggregations;
    }

    public static GroupByField randomGroupByField() {
        GroupByField groupBy = new GroupByField();
        groupBy.setGroupByName(randomString(10));
        groupBy.setFieldName(randomString(10));
        if (RANDOM.nextBoolean()) {
            groupBy.setGroupBySorters(randomGroupBySorterList());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setSize(RANDOM.nextInt());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setSubGroupBys(randomGroupBys());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setSubAggregations(randomAggregations());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setMinDocCount(RANDOM.nextLong());
        }
        assertAllFieldTested(groupBy, 8);
        return groupBy;
    }

    public static GroupByGeoDistance randomGroupByGeoDistance() {
        GroupByGeoDistance groupBy = new GroupByGeoDistance();
        groupBy.setGroupByName(randomString(10));
        groupBy.setFieldName(randomString(10));
        groupBy.setOrigin(new GeoPoint(RANDOM.nextDouble(), RANDOM.nextDouble()));
        List<Range> list = new ArrayList<Range>();
        int i = RANDOM.nextInt(4) + 1;
        for (int i1 = 0; i1 < i; i1++) {
            list.add(new Range(RANDOM.nextDouble(), RANDOM.nextDouble()));
        }
        groupBy.setRanges(list);
        if (RANDOM.nextBoolean()) {
            groupBy.setSubGroupBys(randomGroupBys());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setSubAggregations(randomAggregations());
        }
        assertAllFieldTested(groupBy, 7);
        return groupBy;
    }

    public static GroupByRange randomGroupByRange() {
        GroupByRange groupBy = new GroupByRange();
        groupBy.setGroupByName(randomString(10));
        groupBy.setFieldName(randomString(10));
        List<Range> list = new ArrayList<Range>();
        int i = RANDOM.nextInt(4) + 1;
        for (int i1 = 0; i1 < i; i1++) {
            list.add(new Range(RANDOM.nextDouble(), RANDOM.nextDouble()));
        }
        groupBy.setRanges(list);
        if (RANDOM.nextBoolean()) {
            groupBy.setSubGroupBys(randomGroupBys());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setSubAggregations(randomAggregations());
        }
        assertAllFieldTested(groupBy, 6);
        return groupBy;
    }

    public static ColumnValue randomNumberColumnValue() {
        return RANDOM.nextBoolean() ? ValueUtil.toColumnValue(RANDOM.nextLong()) : ValueUtil.toColumnValue(RANDOM.nextDouble());
    }

    public static ColumnValue randomStringColumnValue() {
        return randomFrom(Arrays.asList(
                ValueUtil.toColumnValue(randomString(10)),
                FieldSort.FIRST_WHEN_MISSING,
                FieldSort.LAST_WHEN_MISSING
        ));
    }

    public static ColumnValue randomBoolColumnValue() {
        return ValueUtil.toColumnValue(RANDOM.nextBoolean());
    }

    public static ColumnValue randomColumnValue() {
        List<Supplier<ColumnValue>> objects = Arrays.asList(
                new Supplier<ColumnValue>() {
                    @Override
                    public ColumnValue get() {
                        return randomNumberColumnValue();
                    }
                },
                new Supplier<ColumnValue>() {
                    @Override
                    public ColumnValue get() {
                        return randomStringColumnValue();
                    }
                },
                new Supplier<ColumnValue>() {
                    @Override
                    public ColumnValue get() {
                        return randomBoolColumnValue();
                    }
                }
        );
        return randomFrom(objects).get();
    }

    public static GroupByHistogram randomGroupByHistogram() {
        GroupByHistogram groupBy = new GroupByHistogram();
        groupBy.setGroupByName(randomString(10));
        groupBy.setFieldName(randomString(10));
        if (RANDOM.nextBoolean()) {
            groupBy.setGroupBySorters(randomGroupBySorterList());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setInterval(randomNumberColumnValue());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setFieldRange(new FieldRange(randomNumberColumnValue(), randomNumberColumnValue()));
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setMinDocCount(RANDOM.nextLong());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setMissing(randomNumberColumnValue());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setOffset(randomNumberColumnValue());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setSubGroupBys(randomGroupBys());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setSubAggregations(randomAggregations());
        }
        assertAllFieldTested(groupBy, 11);
        return groupBy;
    }

    public static DateTimeValue randomDateTimeValue() {
        DateTimeValue dateTimeValue = new DateTimeValue();
        if (RANDOM.nextBoolean()) {
            dateTimeValue.setValue(RANDOM.nextInt());
        }
        if (RANDOM.nextBoolean()) {
            dateTimeValue.setUnit(randomFrom(DateTimeUnit.values()));
        }
        return dateTimeValue;
    }

    public static GroupByField randomGroupByFieldForComposite() {
        GroupByField groupBy = new GroupByField();
        groupBy.setGroupByName(randomString(10));
        groupBy.setFieldName(randomString(10));
        return groupBy;
    }

    public static GroupByHistogram randomGroupByHistogramForComposite() {
        GroupByHistogram groupBy = new GroupByHistogram();
        groupBy.setGroupByName(randomString(10));
        groupBy.setFieldName(randomString(10));
        if (RANDOM.nextBoolean()) {
            groupBy.setInterval(randomNumberColumnValue());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setMissing(randomNumberColumnValue());
        }
        return groupBy;
    }

    public static GroupByDateHistogram randomGroupByDateHistogramForComposite() {
        GroupByDateHistogram groupBy = new GroupByDateHistogram();
        groupBy.setGroupByName(randomString(10));
        groupBy.setFieldName(randomString(10));
        if (RANDOM.nextBoolean()) {
            groupBy.setInterval(randomDateTimeValue());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setMissing(randomNumberColumnValue());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setTimeZone(randomString(10));
        }
        return groupBy;
    }

    public static GroupByComposite randomGroupByComposite() {
        GroupByComposite groupBy = new GroupByComposite();
        groupBy.setGroupByName(randomString(10));
        if (RANDOM.nextBoolean()) {
            groupBy.setSize(RANDOM.nextInt());
        }
        if (RANDOM.nextBoolean()) {
            int sourcesLen = RANDOM.nextInt(10);
            List<GroupBy> sources = new ArrayList<GroupBy>();
            for (int i = 0; i < sourcesLen; i++) {
                if (RANDOM.nextBoolean()) {
                    sources.add(randomGroupByFieldForComposite());
                } else if (RANDOM.nextBoolean()) {
                    sources.add(randomGroupByHistogramForComposite());
                } else {
                    sources.add(randomGroupByDateHistogramForComposite());
                }
            }
            groupBy.setSources(sources);
        }

        if (RANDOM.nextBoolean()) {
            groupBy.setSubGroupBys(randomGroupBys());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setSubAggregations(randomAggregations());
        }

        return groupBy;
    }

    public static GroupByDateHistogram randomGroupByDateHistogram() {
        GroupByDateHistogram groupBy = new GroupByDateHistogram();
        groupBy.setGroupByName(randomString(10));
        groupBy.setFieldName(randomString(10));
        if (RANDOM.nextBoolean()) {
            groupBy.setGroupBySorters(randomGroupBySorterList());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setInterval(randomDateTimeValue());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setFieldRange(new FieldRange(randomNumberColumnValue(), randomNumberColumnValue()));
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setMinDocCount(RANDOM.nextLong());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setMissing(randomNumberColumnValue());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setOffset(randomDateTimeValue());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setTimeZone(randomString(10));
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setSubGroupBys(randomGroupBys());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setSubAggregations(randomAggregations());
        }
        assertAllFieldTested(groupBy, 12);
        return groupBy;
    }

    public static GroupByFilter randomGroupByFilter() {
        GroupByFilter groupBy = new GroupByFilter();
        groupBy.setGroupByName(randomString(10));
        groupBy.setFilters(randomQueries());
        if (RANDOM.nextBoolean()) {
            groupBy.setSubGroupBys(randomGroupBys());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setSubAggregations(randomAggregations());
        }
        assertAllFieldTested(groupBy, 5);
        return groupBy;
    }

    public static GroupByGeoGrid randomGroupByGeoGrid() {
        GroupByGeoGrid groupBy = new GroupByGeoGrid();
        groupBy.setGroupByName(randomString(10));
        groupBy.setFieldName(randomString(10));
        if (RANDOM.nextBoolean()) {
            groupBy.setPrecision(GeoHashPrecision.values()[RANDOM.nextInt(12) + 1]);
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setSize(RANDOM.nextInt());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setSubGroupBys(randomGroupBys());
        }
        if (RANDOM.nextBoolean()) {
            groupBy.setSubAggregations(randomAggregations());
        }
        assertAllFieldTested(groupBy, 8);
        return groupBy;
    }

    public static GroupBy randomGroupBy() {
        return randomFrom(getAllGroupBySupplier()).get();
    }

    public static List<Supplier<GroupBy>> getAllGroupBySupplier() {
        List<Supplier<GroupBy>> all = new ArrayList<Supplier<GroupBy>>();
        all.add(new Supplier<GroupBy>() {
            @Override
            public GroupBy get() {
                return randomGroupByField();
            }
        });
        all.add(new Supplier<GroupBy>() {
            @Override
            public GroupBy get() {
                return randomGroupByFilter();
            }
        });
        all.add(new Supplier<GroupBy>() {
            @Override
            public GroupBy get() {
                return randomGroupByRange();
            }
        });
        all.add(new Supplier<GroupBy>() {
            @Override
            public GroupBy get() {
                return randomGroupByHistogram();
            }
        });
        all.add(new Supplier<GroupBy>() {
            @Override
            public GroupBy get() {
                return randomGroupByDateHistogram();
            }
        });
        all.add(new Supplier<GroupBy>() {
            @Override
            public GroupBy get() {
                return randomGroupByComposite();
            }
        });
        all.add(new Supplier<GroupBy>() {
            @Override
            public GroupBy get() {
                return randomGroupByGeoDistance();
            }
        });
        all.add(new Supplier<GroupBy>() {
            @Override
            public GroupBy get() {
                return randomGroupByGeoGrid();
            }
        });
        return all;
    }

    public static List<GroupBy> randomGroupBys() {
        List<GroupBy> groupBIES = new ArrayList<GroupBy>();
        int i = RANDOM.nextInt(2);
        for (int i1 = 0; i1 < i; i1++) {
            groupBIES.add(randomGroupBy());
        }
        return groupBIES;
    }

    public static SearchQuery randomSearchQuery() {
        SearchQuery searchQuery = new SearchQuery();
        if (RANDOM.nextBoolean()) {
            searchQuery.setOffset(RANDOM.nextInt());
        }
        if (RANDOM.nextBoolean()) {
            searchQuery.setLimit(RANDOM.nextInt());
        }
        if (RANDOM.nextBoolean()) {
            searchQuery.setQuery(randomQuery());
        }
        if (RANDOM.nextBoolean()) {
            searchQuery.setHighlight(randomHighlight());
        }
        if (RANDOM.nextBoolean()) {
            searchQuery.setSort(randomSort());
        }
        if (RANDOM.nextBoolean()) {
            searchQuery.setCollapse(new Collapse(randomString(6)));
        }
        if (RANDOM.nextBoolean()) {
            searchQuery.setGetTotalCount(RANDOM.nextBoolean());
        }
        if (RANDOM.nextBoolean()) {
            searchQuery.setToken(randomString(10).getBytes());
        }
        if (RANDOM.nextBoolean()) {
            searchQuery.setAggregationList(randomAggregations());
        }
        if (RANDOM.nextBoolean()) {
            searchQuery.setGroupByList(randomGroupBys());
        }
        assertAllFieldTested(searchQuery, 10);
        return searchQuery;
    }

    public static PrimaryKey randomPrimaryKey() {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(randomString(RANDOM.nextInt(100) + 1)));
        if (RANDOM.nextBoolean()) {
            primaryKeyBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(RANDOM.nextBoolean() ? RANDOM.nextInt() : RANDOM.nextLong()));
        }
        if (RANDOM.nextBoolean()) {
            primaryKeyBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromBinary(randomString(RANDOM.nextInt(200) + 1).getBytes()));
        }
        return primaryKeyBuilder.build();
    }

    public static List<PrimaryKey> randomPrimaryKeys() {
        List<PrimaryKey> list = new ArrayList<PrimaryKey>();
        int size = RANDOM.nextInt(3) + 1;
        for (int i = 0; i < size; i++) {
            list.add(randomPrimaryKey());
        }
        return list;
    }

    public static SearchRequest randomSearchRequest() {
        SearchRequest searchRequest = new SearchRequest();
        if (RANDOM.nextBoolean()) {
            searchRequest.setTableName(randomString(10));
        }
        if (RANDOM.nextBoolean()) {
            searchRequest.setIndexName(randomString(10));
        }
        if (RANDOM.nextBoolean()) {
            searchRequest.setColumnsToGet(randomColumnsToGet());
        }
        if (RANDOM.nextBoolean()) {
            searchRequest.setSearchQuery(randomSearchQuery());
        }
        if (RANDOM.nextBoolean()) {
            searchRequest.setRoutingValues(randomPrimaryKeys());
        }
        if (RANDOM.nextBoolean()) {
            searchRequest.setTimeoutInMillisecond(RANDOM.nextInt(100000000));
        }
        assertAllFieldTested(searchRequest, 6);
        return searchRequest;
    }

    public static ColumnsToGet randomColumnsToGet() {
        ColumnsToGet columnsToGet = new ColumnsToGet();
        if (RANDOM.nextBoolean()) {
            randomList(new Supplier<String>() {
                @Override
                public String get() {
                    return randomString(RANDOM.nextInt(30) + 1);
                }
            });
        }
        if (RANDOM.nextBoolean()) {
            columnsToGet.setReturnAll(RANDOM.nextBoolean());
        } else if (RANDOM.nextBoolean()) {
            columnsToGet.setReturnAllFromIndex(RANDOM.nextBoolean());
        }
        return columnsToGet;
    }

    public static ScanQuery randomScanQuery() {
        ScanQuery scanQuery = new ScanQuery();
        if (RANDOM.nextBoolean()) {
            scanQuery.setLimit(RANDOM.nextInt());
        }
        if (RANDOM.nextBoolean()) {
            scanQuery.setQuery(randomQuery());
        }
        if (RANDOM.nextBoolean()) {
            scanQuery.setToken(randomString(RANDOM.nextInt(100) + 1).getBytes());
        }
        if (RANDOM.nextBoolean()) {
            scanQuery.setMaxParallel(RANDOM.nextInt());
        }
        if (RANDOM.nextBoolean()) {
            scanQuery.setCurrentParallelId(RANDOM.nextInt());
        }
        if (RANDOM.nextBoolean()) {
            scanQuery.setAliveTime(RANDOM.nextInt(100000000));
        }
        assertAllFieldTested(scanQuery, 6);
        return scanQuery;
    }

    public static ParallelScanRequest randomParallelScanRequest() {
        ParallelScanRequest request = new ParallelScanRequest();
        if (RANDOM.nextBoolean()) {
            request.setTableName(randomString(RANDOM.nextInt(100) + 1));
        }
        if (RANDOM.nextBoolean()) {
            request.setIndexName(randomString(RANDOM.nextInt(100) + 1));
        }
        if (RANDOM.nextBoolean()) {
            request.setColumnsToGet(randomColumnsToGet());
        }
        if (RANDOM.nextBoolean()) {
            request.setSessionId(randomString(RANDOM.nextInt(100) + 1).getBytes());
        }
        if (RANDOM.nextBoolean()) {
            request.setScanQuery(randomScanQuery());
        }
        if (RANDOM.nextBoolean()) {
            request.setTimeoutInMillisecond(RANDOM.nextInt(100000000));
        }
        assertAllFieldTested(request, 6);
        return request;
    }

    public static VectorOptions randomVectorOptions() {
        VectorOptions options = new VectorOptions();
        if (RANDOM.nextBoolean()) {
            options.setDataType(randomFrom(VectorDataType.values()));
        }
        if (RANDOM.nextBoolean()) {
            options.setDimension(RANDOM.nextInt());
        }
        if (RANDOM.nextBoolean()) {
            options.setMetricType(randomFrom(VectorMetricType.values()));
        }
        assertAllFieldTested(options, 3);
        return options;
    }
}

