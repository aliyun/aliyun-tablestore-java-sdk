package com.alicloud.openservices.tablestore.timestream.model.filter;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AttributeUnitTest {
    @Test
    public void testIn() {
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            int count = 10;
            ColumnValue[] valueList = new ColumnValue[count];
            for (int i = 0; i < count; ++i) {
                valueList[i] = ColumnValue.fromString("value_" + now + "_" + i);
            }
            Attribute attribute = Attribute.in(key, valueList);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof TermsQuery);
            Assert.assertEquals(((TermsQuery) query).getTerms().size(), 10);
            Assert.assertEquals(((TermsQuery) query).getFieldName(), key);
            for (int i = 0; i < count; ++i) {
                Assert.assertEquals(((TermsQuery) query).getTerms().get(i), valueList[i]);
            }
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            int count = 10;
            String[] valueList = new String[count];
            for (int i = 0; i < count; ++i) {
                valueList[i] = "value_" + now + "_" + i;
            }
            Attribute attribute = Attribute.in(key, valueList);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof TermsQuery);
            Assert.assertEquals(((TermsQuery) query).getTerms().size(), 10);
            Assert.assertEquals(((TermsQuery) query).getFieldName(), key);
            for (int i = 0; i < count; ++i) {
                Assert.assertEquals(((TermsQuery) query).getTerms().get(i).asString(), valueList[i]);
            }
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            int count = 10;
            long[] valueList = new long[count];
            for (int i = 0; i < count; ++i) {
                valueList[i] = now + i;
            }
            Attribute attribute = Attribute.in(key, valueList);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof TermsQuery);
            Assert.assertEquals(((TermsQuery) query).getTerms().size(), 10);
            Assert.assertEquals(((TermsQuery) query).getFieldName(), key);
            for (int i = 0; i < count; ++i) {
                Assert.assertEquals(((TermsQuery) query).getTerms().get(i).asLong(), valueList[i]);
            }
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            int count = 10;
            byte[][] valueList = new byte[count][3];
            for (int i = 0; i < count; ++i) {
                valueList[i][0] = 1;
                valueList[i][1] = 2;
                valueList[i][1] = (byte)(count % 10);
            }
            Attribute attribute = Attribute.in(key, valueList);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof TermsQuery);
            Assert.assertEquals(((TermsQuery) query).getTerms().size(), 10);
            Assert.assertEquals(((TermsQuery) query).getFieldName(), key);
            for (int i = 0; i < count; ++i) {
                Assert.assertEquals(((TermsQuery) query).getTerms().get(i).asBinary(), valueList[i]);
            }
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            int count = 10;
            double[] valueList = new double[count];
            for (int i = 0; i < count; ++i) {
                valueList[i] = 1.0 * i;
            }
            Attribute attribute = Attribute.in(key, valueList);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof TermsQuery);
            Assert.assertEquals(((TermsQuery) query).getTerms().size(), 10);
            Assert.assertEquals(((TermsQuery) query).getFieldName(), key);
            for (int i = 0; i < count; ++i) {
                Assert.assertTrue(((TermsQuery) query).getTerms().get(i).asDouble() == valueList[i]);
            }
        }
    }

    @Test
    public void testNotIn() {
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            int count = 10;
            ColumnValue[] valueList = new ColumnValue[count];
            for (int i = 0; i < count; ++i) {
                valueList[i] = ColumnValue.fromString("value_" + now + "_" + i);
            }
            Attribute attribute = Attribute.notIn(key, valueList);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof BoolQuery);
            Assert.assertEquals(((BoolQuery) query).getMustNotQueries().size(), 10);
            for (int i = 0; i < count; ++i) {
                Assert.assertTrue(((BoolQuery) query).getMustNotQueries().get(i) instanceof TermQuery);
                TermQuery termQuery = (TermQuery) ((BoolQuery) query).getMustNotQueries().get(i);
                Assert.assertEquals(termQuery.getFieldName(), key);
                Assert.assertEquals(termQuery.getTerm(), valueList[i]);
            }
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            int count = 10;
            String[] valueList = new String[count];
            for (int i = 0; i < count; ++i) {
                valueList[i] = "value_" + now + "_" + i;
            }
            Attribute attribute = Attribute.notIn(key, valueList);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof BoolQuery);
            Assert.assertEquals(((BoolQuery) query).getMustNotQueries().size(), 10);
            for (int i = 0; i < count; ++i) {
                Assert.assertTrue(((BoolQuery) query).getMustNotQueries().get(i) instanceof TermQuery);
                TermQuery termQuery = (TermQuery) ((BoolQuery) query).getMustNotQueries().get(i);
                Assert.assertEquals(termQuery.getFieldName(), key);
                Assert.assertEquals(termQuery.getTerm().asString(), valueList[i]);
            }
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            int count = 10;
            long[] valueList = new long[count];
            for (int i = 0; i < count; ++i) {
                valueList[i] = now + i;
            }
            Attribute attribute = Attribute.notIn(key, valueList);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof BoolQuery);
            Assert.assertEquals(((BoolQuery) query).getMustNotQueries().size(), 10);
            for (int i = 0; i < count; ++i) {
                Assert.assertTrue(((BoolQuery) query).getMustNotQueries().get(i) instanceof TermQuery);
                TermQuery termQuery = (TermQuery) ((BoolQuery) query).getMustNotQueries().get(i);
                Assert.assertEquals(termQuery.getFieldName(), key);
                Assert.assertEquals(termQuery.getTerm().asLong(), valueList[i]);
            }
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            int count = 10;
            byte[][] valueList = new byte[count][3];
            for (int i = 0; i < count; ++i) {
                valueList[i][0] = 1;
                valueList[i][1] = 2;
                valueList[i][1] = (byte)(count % 10);
            }
            Attribute attribute = Attribute.notIn(key, valueList);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof BoolQuery);
            Assert.assertEquals(((BoolQuery) query).getMustNotQueries().size(), 10);
            for (int i = 0; i < count; ++i) {
                Assert.assertTrue(((BoolQuery) query).getMustNotQueries().get(i) instanceof TermQuery);
                TermQuery termQuery = (TermQuery) ((BoolQuery) query).getMustNotQueries().get(i);
                Assert.assertEquals(termQuery.getFieldName(), key);
                Assert.assertEquals(termQuery.getTerm().asBinary(), valueList[i]);
            }
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            int count = 10;
            double[] valueList = new double[count];
            for (int i = 0; i < count; ++i) {
                valueList[i] = 1.0 * i;
            }
            Attribute attribute = Attribute.notIn(key, valueList);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof BoolQuery);
            Assert.assertEquals(((BoolQuery) query).getMustNotQueries().size(), 10);
            for (int i = 0; i < count; ++i) {
                Assert.assertTrue(((BoolQuery) query).getMustNotQueries().get(i) instanceof TermQuery);
                TermQuery termQuery = (TermQuery) ((BoolQuery) query).getMustNotQueries().get(i);
                Assert.assertEquals(termQuery.getFieldName(), key);
                Assert.assertTrue(termQuery.getTerm().asDouble() == valueList[i]);
            }
        }
    }

    @Test
    public void testEqual() {
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            ColumnValue value = ColumnValue.fromString("value_" + now);
            Attribute attribute = Attribute.equal(key, value);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof TermQuery);
            TermQuery termQuery = (TermQuery) query;
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getTerm(), value);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            String value = "value_" + now;
            Attribute attribute = Attribute.equal(key, value);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof TermQuery);
            TermQuery termQuery = (TermQuery) query;
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getTerm().asString(), value);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            long value = now;
            Attribute attribute = Attribute.equal(key, value);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof TermQuery);
            TermQuery termQuery = (TermQuery) query;
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getTerm().asLong(), value);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            byte[] value = new byte[3];
            value[0] = 1;
            value[1] = 2;
            value[2] = 3;
            Attribute attribute = Attribute.equal(key, value);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof TermQuery);
            TermQuery termQuery = (TermQuery) query;
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getTerm().asBinary(), value);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            double value = 1.0 * now;
            Attribute attribute = Attribute.equal(key, value);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof TermQuery);
            TermQuery termQuery = (TermQuery) query;
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertTrue(termQuery.getTerm().asDouble() == value);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            boolean value = true;
            Attribute attribute = Attribute.equal(key, value);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof TermQuery);
            TermQuery termQuery = (TermQuery) query;
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getTerm().asBoolean(), value);
        }
    }

    @Test
    public void testNotEqual() {
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            ColumnValue value = ColumnValue.fromString("value_" + now);
            Attribute attribute = Attribute.notEqual(key, value);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof BoolQuery);
            Assert.assertEquals(((BoolQuery) query).getMustNotQueries().size(), 1);
            Assert.assertTrue(((BoolQuery) query).getMustNotQueries().get(0) instanceof TermQuery);
            TermQuery termQuery = (TermQuery) ((BoolQuery) query).getMustNotQueries().get(0);
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getTerm(), value);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            String value = "value_" + now;
            Attribute attribute = Attribute.notEqual(key, value);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof BoolQuery);
            Assert.assertEquals(((BoolQuery) query).getMustNotQueries().size(), 1);
            Assert.assertTrue(((BoolQuery) query).getMustNotQueries().get(0) instanceof TermQuery);
            TermQuery termQuery = (TermQuery) ((BoolQuery) query).getMustNotQueries().get(0);
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getTerm().asString(), value);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            long value = now;
            Attribute attribute = Attribute.notEqual(key, value);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof BoolQuery);
            Assert.assertEquals(((BoolQuery) query).getMustNotQueries().size(), 1);
            Assert.assertTrue(((BoolQuery) query).getMustNotQueries().get(0) instanceof TermQuery);
            TermQuery termQuery = (TermQuery) ((BoolQuery) query).getMustNotQueries().get(0);
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getTerm().asLong(), value);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            byte[] value = new byte[3];
            value[0] = 1;
            value[1] = 2;
            value[2] = 3;
            Attribute attribute = Attribute.notEqual(key, value);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof BoolQuery);
            Assert.assertEquals(((BoolQuery) query).getMustNotQueries().size(), 1);
            Assert.assertTrue(((BoolQuery) query).getMustNotQueries().get(0) instanceof TermQuery);
            TermQuery termQuery = (TermQuery) ((BoolQuery) query).getMustNotQueries().get(0);
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getTerm().asBinary(), value);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            double value = 1.0 * now;
            Attribute attribute = Attribute.notEqual(key, value);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof BoolQuery);
            Assert.assertEquals(((BoolQuery) query).getMustNotQueries().size(), 1);
            Assert.assertTrue(((BoolQuery) query).getMustNotQueries().get(0) instanceof TermQuery);
            TermQuery termQuery = (TermQuery) ((BoolQuery) query).getMustNotQueries().get(0);
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertTrue(termQuery.getTerm().asDouble() == value);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            boolean value = true;
            Attribute attribute = Attribute.notEqual(key, value);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof BoolQuery);
            Assert.assertEquals(((BoolQuery) query).getMustNotQueries().size(), 1);
            Assert.assertTrue(((BoolQuery) query).getMustNotQueries().get(0) instanceof TermQuery);
            TermQuery termQuery = (TermQuery) ((BoolQuery) query).getMustNotQueries().get(0);
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getTerm().asBoolean(), value);
        }
    }

    @Test
    public void testInRange() {
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            ColumnValue begin = ColumnValue.fromString("value_" + now);
            ColumnValue end = ColumnValue.fromString("value_" + now + 1);
            Attribute attribute = Attribute.inRange(key, begin, end);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof RangeQuery);
            RangeQuery termQuery = (RangeQuery) query;
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getFrom(), begin);
            Assert.assertEquals(termQuery.getTo(), end);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            String begin = "value_" + now;
            String end = "value_" + now + 1;
            Attribute attribute = Attribute.inRange(key, begin, end);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof RangeQuery);
            RangeQuery termQuery = (RangeQuery) query;
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getFrom().asString(), begin);
            Assert.assertEquals(termQuery.getTo().asString(), end);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            long begin = now;
            long end = now + 1;
            Attribute attribute = Attribute.inRange(key, begin, end);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof RangeQuery);
            RangeQuery termQuery = (RangeQuery) query;
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getFrom().asLong(), begin);
            Assert.assertEquals(termQuery.getTo().asLong(), end);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            byte[] begin = new byte[1];
            begin[0] = 1;
            byte[] end = new byte[1];
            end[0] = 2;
            Attribute attribute = Attribute.inRange(key, begin, end);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof RangeQuery);
            RangeQuery termQuery = (RangeQuery) query;
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getFrom().asBinary(), begin);
            Assert.assertEquals(termQuery.getTo().asBinary(), end);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            double begin = now;
            double end = now + 1;
            Attribute attribute = Attribute.inRange(key, begin, end);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof RangeQuery);
            RangeQuery termQuery = (RangeQuery) query;
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertTrue(termQuery.getFrom().asDouble() == begin);
            Assert.assertTrue(termQuery.getTo().asDouble() == end);
        }
    }

    @Test
    public void testPrefix() {
        long now = System.currentTimeMillis();
        String key = "key_" + now;
        String value = "value_" + now;
        Attribute attribute = Attribute.prefix(key, value);
        Query query = attribute.getQuery();
        Assert.assertTrue(query instanceof PrefixQuery);
        PrefixQuery termQuery = (PrefixQuery)query;
        Assert.assertEquals(termQuery.getFieldName(), key);
        Assert.assertEquals(termQuery.getPrefix(), value);
    }

    @Test
    public void testWildcard() {
        long now = System.currentTimeMillis();
        String key = "key_" + now;
        String value = "value_" + now;
        Attribute attribute = Attribute.wildcard(key, value);
        Query query = attribute.getQuery();
        Assert.assertTrue(query instanceof WildcardQuery);
        WildcardQuery termQuery = (WildcardQuery)query;
        Assert.assertEquals(termQuery.getFieldName(), key);
        Assert.assertEquals(termQuery.getValue(), value);
    }

    @Test
    public void testInGeoPolygon() {
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            List<String> points = new ArrayList<String>();
            points.add("123,456");
            Attribute attribute = Attribute.inGeoPolygon(key, points);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof GeoPolygonQuery);
            GeoPolygonQuery termQuery = (GeoPolygonQuery)query;
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getPoints(), points);
        }
        {
            long now = System.currentTimeMillis();
            String key = "key_" + now;
            String[] points = new String[3];
            points[0] = "123,456";
            points[1] = "234,567";
            points[2] = "345,678";
            Attribute attribute = Attribute.inGeoPolygon(key, points);
            Query query = attribute.getQuery();
            Assert.assertTrue(query instanceof GeoPolygonQuery);
            GeoPolygonQuery termQuery = (GeoPolygonQuery)query;
            Assert.assertEquals(termQuery.getFieldName(), key);
            Assert.assertEquals(termQuery.getPoints().size(), 3);
            Assert.assertEquals(termQuery.getPoints().get(0), points[0]);
            Assert.assertEquals(termQuery.getPoints().get(1), points[1]);
            Assert.assertEquals(termQuery.getPoints().get(2), points[2]);
        }
    }

    @Test
    public void testInGeoDistance() {
        long now = System.currentTimeMillis();
        String key = "key_" + now;
        String pos = "123,456";
        double distance = 1.0;
        Attribute attribute = Attribute.inGeoDistance(key, pos, distance);
        Query query = attribute.getQuery();
        Assert.assertTrue(query instanceof GeoDistanceQuery);
        GeoDistanceQuery termQuery = (GeoDistanceQuery)query;
        Assert.assertEquals(termQuery.getFieldName(), key);
        Assert.assertEquals(termQuery.getCenterPoint(), pos);
        Assert.assertTrue(termQuery.getDistanceInMeter() == distance);
    }

    @Test
    public void testInGeoBoundingBox() {
        long now = System.currentTimeMillis();
        String key = "key_" + now;
        String left = "123,456";
        String right = "234,567";
        Attribute attribute = Attribute.inGeoBoundingBox(key, left, right);
        Query query = attribute.getQuery();
        Assert.assertTrue(query instanceof GeoBoundingBoxQuery);
        GeoBoundingBoxQuery termQuery = (GeoBoundingBoxQuery)query;
        Assert.assertEquals(termQuery.getFieldName(), key);
        Assert.assertEquals(termQuery.getTopLeft(), left);
        Assert.assertEquals(termQuery.getBottomRight(), right);
    }
}
