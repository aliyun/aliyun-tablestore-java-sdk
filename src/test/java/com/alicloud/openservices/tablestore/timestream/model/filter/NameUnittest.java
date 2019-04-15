package com.alicloud.openservices.tablestore.timestream.model.filter;

import com.alicloud.openservices.tablestore.model.search.query.*;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import org.junit.Assert;
import org.junit.Test;

public class NameUnittest {
    @Test
    public void testIn() {
        long now = System.currentTimeMillis();
        int count = 10;
        String[] valueList = new String[count];
        for (int i = 0; i < count; ++i) {
            valueList[i] =  "value_" + now + "_" + i;
        }
        Name name = Name.in(valueList);
        Query query = name.getQuery();
        Assert.assertTrue(query instanceof TermsQuery);
        Assert.assertEquals(((TermsQuery)query).getTerms().size(), 10);
        Assert.assertEquals(((TermsQuery)query).getFieldName(), TableMetaGenerator.CN_PK1);
        for (int i = 0; i < count; ++i) {
            Assert.assertEquals(((TermsQuery)query).getTerms().get(i).asString(), valueList[i]);
        }
    }

    @Test
    public void testNotIn() {
        long now = System.currentTimeMillis();
        int count = 10;
        String[] valueList = new String[count];
        for (int i = 0; i < count; ++i) {
            valueList[i] =  "value_" + now + "_" + i;
        }
        Name name = Name.notIn(valueList);
        Query query = name.getQuery();
        Assert.assertTrue(query instanceof BoolQuery);
        Assert.assertEquals(((BoolQuery)query).getMustNotQueries().size(), 10);
        for (int i = 0; i < count; ++i) {
            Assert.assertTrue(((BoolQuery) query).getMustNotQueries().get(i) instanceof TermQuery);
            TermQuery termQuery = (TermQuery) ((BoolQuery) query).getMustNotQueries().get(i);
            Assert.assertEquals(termQuery.getFieldName(), TableMetaGenerator.CN_PK1);
            Assert.assertEquals(termQuery.getTerm().asString(), valueList[i]);
        }
    }

    @Test
    public void testEqual() {
        long now = System.currentTimeMillis();
        String value = "value_" + now;
        Name name = Name.equal(value);
        Query query = name.getQuery();
        Assert.assertTrue(query instanceof TermQuery);
        TermQuery termQuery = (TermQuery)query;
        Assert.assertEquals(termQuery.getFieldName(), TableMetaGenerator.CN_PK1);
        Assert.assertEquals(termQuery.getTerm().asString(), value);
    }

    @Test
    public void testNotEqual() {
        long now = System.currentTimeMillis();
        String value = "value_" + now;
        Name name = Name.notEqual(value);
        Query query = name.getQuery();
        Assert.assertTrue(query instanceof BoolQuery);
        Assert.assertEquals(((BoolQuery)query).getMustNotQueries().size(), 1);
        Assert.assertTrue(((BoolQuery)query).getMustNotQueries().get(0) instanceof TermQuery);
        TermQuery termQuery = (TermQuery)((BoolQuery)query).getMustNotQueries().get(0);
        Assert.assertEquals(termQuery.getFieldName(), TableMetaGenerator.CN_PK1);
        Assert.assertEquals(termQuery.getTerm().asString(), value);
    }

    @Test
    public void testPrefix() {
        long now = System.currentTimeMillis();
        String value = "value_" + now;
        Name name = Name.prefix(value);
        Query query = name.getQuery();
        Assert.assertTrue(query instanceof PrefixQuery);
        PrefixQuery termQuery = (PrefixQuery)query;
        Assert.assertEquals(termQuery.getFieldName(), TableMetaGenerator.CN_PK1);
        Assert.assertEquals(termQuery.getPrefix(), value);
    }
}
