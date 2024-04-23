package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.core.protocol.SearchGroupByBuilder;
import com.alicloud.openservices.tablestore.core.protocol.SearchVariantType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.DateTimeUnit;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilders;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

public class GroupByCompositeTest extends BaseSearchTest {
    @Test
    public void testBuildGroupByComposite() throws InvalidProtocolBufferException {
        GroupByComposite.Builder builder = GroupByBuilders.groupByComposite("groupByComposite")
                .nextToken("test")
                .size(1000)
                .suggestedSize(10000);
        builder.setSources(Arrays.asList(
                GroupByBuilders.groupByField("groupByField", "Col1").build(),
                GroupByBuilders.groupByHistogram("groupByHistogram", "Col2").interval(10).build(),
                GroupByBuilders.groupByDateHistogram("groupByDateHistogram", "Col3").interval(1, DateTimeUnit.DAY).build()));
        builder.addSubGroupBy(GroupByBuilders.groupByField("subGroupByField", "Sub_Col1"));
        builder.addSubGroupBy(GroupByBuilders.groupByField("subGroupByHistogram", "Sub_Col2"));
        builder.addSubAggregation(AggregationBuilders.max("maxAgg", "Col4"));
        Search.GroupByComposite pbGroupByComposite = SearchGroupByBuilder.buildGroupByComposite(builder.build());

        assertNotNull(pbGroupByComposite.getNextToken());
        assertEquals(pbGroupByComposite.getNextToken(), "test");

        assertTrue(pbGroupByComposite.hasSize());
        assertEquals(1000, pbGroupByComposite.getSize());
        assertTrue(pbGroupByComposite.hasSuggestedSize());
        assertEquals(10000, pbGroupByComposite.getSuggestedSize());

        // source check
        assertEquals(3, pbGroupByComposite.getSources().getGroupBysCount());
        assertEquals("groupByField", pbGroupByComposite.getSources().getGroupBys(0).getName());
        assertEquals(Search.GroupByType.GROUP_BY_FIELD, pbGroupByComposite.getSources().getGroupBys(0).getType());
        Search.GroupByField groupByField = Search.GroupByField.parseFrom(pbGroupByComposite.getSources().getGroupBys(0).getBody());
        assertEquals("Col1", groupByField.getFieldName());
        assertFalse(groupByField.hasSize());
        assertFalse(groupByField.hasMinDocCount());
        assertFalse(groupByField.hasSort());
        assertFalse(groupByField.hasSubAggs());
        assertFalse(groupByField.hasSubGroupBys());

        assertEquals("groupByHistogram", pbGroupByComposite.getSources().getGroupBys(1).getName());
        assertEquals(Search.GroupByType.GROUP_BY_HISTOGRAM, pbGroupByComposite.getSources().getGroupBys(1).getType());
        Search.GroupByHistogram groupByHistogram = Search.GroupByHistogram.parseFrom(pbGroupByComposite.getSources().getGroupBys(1).getBody());
        assertEquals("Col2", groupByHistogram.getFieldName());
        assertFalse(groupByHistogram.hasSort());
        assertFalse(groupByHistogram.hasMinDocCount());
        assertFalse(groupByHistogram.hasSubGroupBys());
        assertFalse(groupByHistogram.hasSubAggs());
        assertFalse(groupByHistogram.hasFieldRange());
        assertFalse(groupByHistogram.hasOffset());
        assertFalse(groupByHistogram.hasMissing());
        assertEquals(ByteString.copyFrom(SearchVariantType.toVariant(ColumnValue.fromLong(10))), groupByHistogram.getInterval());

        assertEquals("groupByDateHistogram", pbGroupByComposite.getSources().getGroupBys(2).getName());
        assertEquals(Search.GroupByType.GROUP_BY_DATE_HISTOGRAM, pbGroupByComposite.getSources().getGroupBys(2).getType());
        Search.GroupByDateHistogram groupByDateHistogram = Search.GroupByDateHistogram.parseFrom(pbGroupByComposite.getSources().getGroupBys(2).getBody());
        assertEquals("Col3", groupByDateHistogram.getFieldName());
        assertFalse(groupByDateHistogram.hasMissing());
        assertFalse(groupByDateHistogram.hasOffset());
        assertFalse(groupByDateHistogram.hasSort());
        assertFalse(groupByDateHistogram.hasFieldRange());
        assertFalse(groupByDateHistogram.hasSubGroupBys());
        assertFalse(groupByDateHistogram.hasSubAggs());
        assertFalse(groupByDateHistogram.hasMinDocCount());
        assertFalse(groupByDateHistogram.hasTimeZone());
        assertEquals(Search.DateTimeValue.newBuilder().setValue(1).setUnit(Search.DateTimeUnit.DAY).build(), groupByDateHistogram.getInterval());

        // check sub aggs & groupBys
        assertTrue(pbGroupByComposite.hasSubGroupBys());
        assertEquals(2, pbGroupByComposite.getSubGroupBys().getGroupBysCount());
        assertEquals("subGroupByField", pbGroupByComposite.getSubGroupBys().getGroupBys(0).getName());
        assertEquals(Search.GroupByType.GROUP_BY_FIELD, pbGroupByComposite.getSubGroupBys().getGroupBys(0).getType());
        groupByField = Search.GroupByField.parseFrom(pbGroupByComposite.getSubGroupBys().getGroupBys(0).getBody());
        assertEquals("Sub_Col1", groupByField.getFieldName());
        assertEquals("subGroupByHistogram", pbGroupByComposite.getSubGroupBys().getGroupBys(1).getName());
        assertEquals(Search.GroupByType.GROUP_BY_FIELD, pbGroupByComposite.getSubGroupBys().getGroupBys(1).getType());
        groupByHistogram = Search.GroupByHistogram.parseFrom(pbGroupByComposite.getSubGroupBys().getGroupBys(1).getBody());
        assertEquals("Sub_Col2", groupByHistogram.getFieldName());
        assertEquals(1, pbGroupByComposite.getSubAggs().getAggsCount());
        assertEquals("maxAgg", pbGroupByComposite.getSubAggs().getAggs(0).getName());
        assertEquals(Search.AggregationType.AGG_MAX, pbGroupByComposite.getSubAggs().getAggs(0).getType());
        Search.SumAggregation sumAggregation = Search.SumAggregation.parseFrom(pbGroupByComposite.getSubAggs().getAggs(0).getBody());
        assertEquals("Col4", sumAggregation.getFieldName());
    }
}
