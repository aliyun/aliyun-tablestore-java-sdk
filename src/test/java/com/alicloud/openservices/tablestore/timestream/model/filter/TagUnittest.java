package com.alicloud.openservices.tablestore.timestream.model.filter;

import com.alicloud.openservices.tablestore.model.search.query.*;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import com.alicloud.openservices.tablestore.timestream.model.expression.EqualExpression;
import com.alicloud.openservices.tablestore.timestream.model.expression.NotInExpression;
import org.junit.Assert;
import org.junit.Test;

public class TagUnittest {
    @Test
    public void testIn() {
        long now = System.currentTimeMillis();
        String key = "key_" + now;
        int count = 10;
        String[] valueList = new String[count];
        for (int i = 0; i < count; ++i) {
            valueList[i] =  "value_" + now + "_" + i;
        }
        Tag tag = Tag.in(key, valueList);
        Query query = tag.getQuery();
        Assert.assertTrue(query instanceof TermsQuery);
        Assert.assertEquals(((TermsQuery)query).getTerms().size(), 10);
        Assert.assertEquals(((TermsQuery)query).getFieldName(), TableMetaGenerator.CN_PK2);
        for (int i = 0; i < count; ++i) {
            Assert.assertEquals(((TermsQuery)query).getTerms().get(i).asString(), Utils.buildTagValue(key, valueList[i]));
        }
    }

    @Test
    public void testNotIn() {
        long now = System.currentTimeMillis();
        String key = "key_" + now;
        int count = 10;
        String[] valueList = new String[count];
        for (int i = 0; i < count; ++i) {
            valueList[i] =  "value_" + now + "_" + i;
        }
        Tag tag = Tag.notIn(key, valueList);
        Query query = tag.getQuery();
        Assert.assertTrue(query instanceof BoolQuery);
        Assert.assertEquals(((BoolQuery)query).getMustNotQueries().size(), 10);
        for (int i = 0; i < count; ++i) {
            Assert.assertTrue(((BoolQuery) query).getMustNotQueries().get(i) instanceof TermQuery);
            TermQuery termQuery = (TermQuery) ((BoolQuery) query).getMustNotQueries().get(i);
            Assert.assertEquals(termQuery.getFieldName(), TableMetaGenerator.CN_PK2);
            Assert.assertEquals(termQuery.getTerm().asString(), Utils.buildTagValue(key, valueList[i]));
        }
    }

    @Test
    public void testEqual() {
        long now = System.currentTimeMillis();
        String key = "key_" + now;
        String value = "value_" + now;
        Tag tag = Tag.equal(key, value);
        Query query = tag.getQuery();
        Assert.assertTrue(query instanceof TermQuery);
        TermQuery termQuery = (TermQuery)query;
        Assert.assertEquals(termQuery.getFieldName(), TableMetaGenerator.CN_PK2);
        Assert.assertEquals(termQuery.getTerm().asString(), Utils.buildTagValue(key, value));
    }

    @Test
    public void testNotEqual() {
        long now = System.currentTimeMillis();
        String key = "key_" + now;
        String value = "value_" + now;
        Tag tag = Tag.notEqual(key, value);
        Query query = tag.getQuery();
        Assert.assertTrue(query instanceof BoolQuery);
        Assert.assertEquals(((BoolQuery)query).getMustNotQueries().size(), 1);
        Assert.assertTrue(((BoolQuery)query).getMustNotQueries().get(0) instanceof TermQuery);
        TermQuery termQuery = (TermQuery)((BoolQuery)query).getMustNotQueries().get(0);
        Assert.assertEquals(termQuery.getFieldName(), TableMetaGenerator.CN_PK2);
        Assert.assertEquals(termQuery.getTerm().asString(), Utils.buildTagValue(key, value));
    }

    @Test
    public void testPrefix() {
        long now = System.currentTimeMillis();
        String key = "key_" + now;
        String value = "value_" + now;
        Tag tag = Tag.prefix(key, value);
        Query query = tag.getQuery();
        Assert.assertTrue(query instanceof PrefixQuery);
        PrefixQuery termQuery = (PrefixQuery)query;
        Assert.assertEquals(termQuery.getFieldName(), TableMetaGenerator.CN_PK2);
        Assert.assertEquals(termQuery.getPrefix(), Utils.buildTagValue(key, value));
    }
}
