package com.alicloud.openservices.tablestore.model.filter;

import com.alicloud.openservices.tablestore.core.protocol.OtsFilter;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;

import org.junit.Test;

import com.google.protobuf.ByteString;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestSingleColumnValueRegexFilter {

    @Test
    public void testSetExistComparator() {
        RegexRule rule = new RegexRule("xx^regex", RegexRule.CastType.VT_STRING);
        SingleColumnValueRegexFilter filter = new SingleColumnValueRegexFilter(
                "column",
                rule,
                SingleColumnValueRegexFilter.CompareOperator.EXIST);
        assertEquals(filter.getOperator(), SingleColumnValueRegexFilter.CompareOperator.EXIST);
    }

    @Test
    public void testSetValueTransferRule() {
        RegexRule rule = new RegexRule("xx^regex", RegexRule.CastType.VT_STRING);
        SingleColumnValueRegexFilter filter = new SingleColumnValueRegexFilter(
                "column",
                rule,
                SingleColumnValueRegexFilter.CompareOperator.EXIST);
        assertEquals(filter.hasRegexRule(), true);
        RegexRule rule1 = filter.getRegexRule();
        assertEquals(rule1.getCastType(), RegexRule.CastType.VT_STRING);
        assertEquals(rule1.getRegex(), "xx^regex");
    }

    @Test
    public void testValueTransferRuleSerial() {
        RegexRule rule = new RegexRule("TEST001xx^regex", RegexRule.CastType.VT_DOUBLE);
        SingleColumnValueRegexFilter filter = new SingleColumnValueRegexFilter(
                "column001",
                rule,
                SingleColumnValueRegexFilter.CompareOperator.EXIST);
        RegexRule rule1 = filter.getRegexRule();
        OtsFilter.ValueTransferRule pbRule = rule.serialize();
        System.out.println(pbRule.toString());
        assertEquals(pbRule.getRegex(), "TEST001xx^regex");
        assertEquals(pbRule.getCastType(), OtsFilter.VariantType.VT_DOUBLE);
    }

    @Test
    public void testBuildPBFilter() {
        RegexRule rule = new RegexRule("BuildPBFilter:^regex", RegexRule.CastType.VT_DOUBLE);
        SingleColumnValueRegexFilter filter = new SingleColumnValueRegexFilter(
                "column002",
                rule,
                SingleColumnValueRegexFilter.CompareOperator.NOT_EXIST);
        ByteString pbString = OTSProtocolBuilder.buildSingleColumnValueRegexFilter(filter);
        try {
            OtsFilter.SingleColumnValueFilter filter1 = OtsFilter.SingleColumnValueFilter.parseFrom(pbString);
            System.out.println(filter1.toString());
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {}
    }

    @Test
    public void testValueCompare() {
        RegexRule rule = new RegexRule("xxx([0-9]+)yyy", RegexRule.CastType.VT_INTEGER);
        SingleColumnValueRegexFilter filter = new SingleColumnValueRegexFilter(
                "columnA",
                rule,
                SingleColumnValueRegexFilter.CompareOperator.GREATER_EQUAL,
                ColumnValue.fromLong(5858));
        ByteString pbString = OTSProtocolBuilder.buildSingleColumnValueRegexFilter(filter);
        try {
            OtsFilter.SingleColumnValueFilter filter1 = OtsFilter.SingleColumnValueFilter.parseFrom(pbString);
            System.out.println(filter1.toString());
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            assertEquals(0, 1);
        }
    }

    @Test
    public void testValueTransferSuccessExpectancy() {
        RegexRule rule = new RegexRule("xxx([0-9]+)yyy", RegexRule.CastType.VT_INTEGER);
        SingleColumnValueRegexFilter filter = new SingleColumnValueRegexFilter(
                "columnA",
                rule,
                SingleColumnValueRegexFilter.CompareOperator.EXIST);

        SingleColumnValueRegexFilter filter1 = new SingleColumnValueRegexFilter(
                "columnA",
                SingleColumnValueRegexFilter.CompareOperator.NOT_EXIST);

        CompositeColumnValueFilter cfilter = new CompositeColumnValueFilter(
                CompositeColumnValueFilter.LogicOperator.OR);
        cfilter.addFilter(filter);
        cfilter.addFilter(filter1);
        ByteString pbString = OTSProtocolBuilder.buildCompositeColumnValueFilter(cfilter);
        try {
            OtsFilter.CompositeColumnValueFilter cfiler = OtsFilter.CompositeColumnValueFilter.parseFrom(pbString);
            System.out.println(cfiler.toString());

            assertEquals(cfiler.getSubFiltersCount(), 2);
            for (OtsFilter.Filter f : cfiler.getSubFiltersList()) {
                assertEquals(f.getType(), OtsFilter.FilterType.FT_SINGLE_COLUMN_VALUE);
                System.out.println("SubFilter:");
                ByteString pbStr = f.getFilter();
                OtsFilter.SingleColumnValueFilter f1 = OtsFilter.SingleColumnValueFilter.parseFrom(pbStr);
                System.out.println(f1.toString());
                if (f1.hasValueTransRule()) {
                    assertEquals(f1.getComparator(), OtsFilter.ComparatorType.CT_EXIST);
                } else {
                    assertEquals(f1.getComparator(), OtsFilter.ComparatorType.CT_NOT_EXIST);
                }
            }
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {}
    }

    @Test
    public void testValueTransferFailExpectancy() {
        RegexRule rule = new RegexRule("xxx([0-9]+)yyy", RegexRule.CastType.VT_DOUBLE);
        SingleColumnValueRegexFilter filter = new SingleColumnValueRegexFilter(
                "columnA",
                rule,
                SingleColumnValueRegexFilter.CompareOperator.NOT_EXIST);

        SingleColumnValueRegexFilter filter1 = new SingleColumnValueRegexFilter(
                "columnA",
                SingleColumnValueRegexFilter.CompareOperator.NOT_EXIST);

        CompositeColumnValueFilter cfilter = new CompositeColumnValueFilter(
                CompositeColumnValueFilter.LogicOperator.OR);
        cfilter.addFilter(filter);
        cfilter.addFilter(filter1);
        ByteString pbString = OTSProtocolBuilder.buildCompositeColumnValueFilter(cfilter);
        try {
            OtsFilter.CompositeColumnValueFilter cfiler = OtsFilter.CompositeColumnValueFilter.parseFrom(pbString);
            System.out.println(cfiler.toString());

            assertEquals(cfiler.getSubFiltersCount(), 2);
            for (OtsFilter.Filter f : cfiler.getSubFiltersList()) {
                assertEquals(f.getType(), OtsFilter.FilterType.FT_SINGLE_COLUMN_VALUE);
                System.out.println("SubFilter:");
                ByteString pbStr = f.getFilter();
                OtsFilter.SingleColumnValueFilter f1 = OtsFilter.SingleColumnValueFilter.parseFrom(pbStr);
                System.out.println(f1.toString());
                if (f1.hasValueTransRule()) {
                    assertEquals(f1.getComparator(), OtsFilter.ComparatorType.CT_NOT_EXIST);
                } else {
                    assertEquals(f1.getComparator(), OtsFilter.ComparatorType.CT_NOT_EXIST);
                }
            }
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {}
    }

    @Test
    public void testValueTransferSuccessExpectancyOnly() {
        RegexRule rule = new RegexRule("xxx([0-9]+)yyy", RegexRule.CastType.VT_INTEGER);
        SingleColumnValueRegexFilter filter = new SingleColumnValueRegexFilter(
                "columnA",
                rule,
                SingleColumnValueRegexFilter.CompareOperator.EXIST);

        SingleColumnValueRegexFilter filter1 = new SingleColumnValueRegexFilter(
                "columnA",
                SingleColumnValueRegexFilter.CompareOperator.EXIST);

        CompositeColumnValueFilter cfilter = new CompositeColumnValueFilter(
                CompositeColumnValueFilter.LogicOperator.AND);
        cfilter.addFilter(filter);
        cfilter.addFilter(filter1);
        ByteString pbString = OTSProtocolBuilder.buildCompositeColumnValueFilter(cfilter);
        try {
            OtsFilter.CompositeColumnValueFilter cfiler = OtsFilter.CompositeColumnValueFilter.parseFrom(pbString);
            System.out.println(cfiler.toString());

            assertEquals(cfiler.getSubFiltersCount(), 2);
            for (OtsFilter.Filter f : cfiler.getSubFiltersList()) {
                assertEquals(f.getType(), OtsFilter.FilterType.FT_SINGLE_COLUMN_VALUE);
                System.out.println("SubFilter:");
                ByteString pbStr = f.getFilter();
                OtsFilter.SingleColumnValueFilter f1 = OtsFilter.SingleColumnValueFilter.parseFrom(pbStr);
                System.out.println(f1.toString());
                if (f1.hasValueTransRule()) {
                    assertEquals(f1.getComparator(), OtsFilter.ComparatorType.CT_EXIST);
                } else {
                    assertEquals(f1.getComparator(), OtsFilter.ComparatorType.CT_EXIST);
                }
            }
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {}
    }

    @Test
    public void testValueTransferFailExpectancyOnly() {
        RegexRule rule = new RegexRule("xxx([0-9]+)yyy", RegexRule.CastType.VT_DOUBLE);
        SingleColumnValueRegexFilter filter = new SingleColumnValueRegexFilter(
                "columnA",
                rule,
                SingleColumnValueRegexFilter.CompareOperator.NOT_EXIST);

        SingleColumnValueRegexFilter filter1 = new SingleColumnValueRegexFilter(
                "columnA",
                SingleColumnValueRegexFilter.CompareOperator.EXIST);

        CompositeColumnValueFilter cfilter = new CompositeColumnValueFilter(
                CompositeColumnValueFilter.LogicOperator.AND);
        cfilter.addFilter(filter);
        cfilter.addFilter(filter1);
        ByteString pbString = OTSProtocolBuilder.buildCompositeColumnValueFilter(cfilter);
        try {
            OtsFilter.CompositeColumnValueFilter cfiler = OtsFilter.CompositeColumnValueFilter.parseFrom(pbString);
            System.out.println(cfiler.toString());

            assertEquals(cfiler.getSubFiltersCount(), 2);
            for (OtsFilter.Filter f : cfiler.getSubFiltersList()) {
                assertEquals(f.getType(), OtsFilter.FilterType.FT_SINGLE_COLUMN_VALUE);
                System.out.println("SubFilter:");
                ByteString pbStr = f.getFilter();
                OtsFilter.SingleColumnValueFilter f1 = OtsFilter.SingleColumnValueFilter.parseFrom(pbStr);
                System.out.println(f1.toString());
                if (f1.hasValueTransRule()) {
                    assertEquals(f1.getComparator(), OtsFilter.ComparatorType.CT_NOT_EXIST);
                } else {
                    assertEquals(f1.getComparator(), OtsFilter.ComparatorType.CT_EXIST);
                }
            }
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {}
    }
}
