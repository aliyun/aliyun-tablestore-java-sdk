package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.model.search.query.ExistsQuery;
import com.alicloud.openservices.tablestore.model.search.query.QueryType;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestExistsQuery extends BaseSearchTest {
    @Test
    public void testGetFieldName() {
        ExistsQuery existsQuery = new ExistsQuery();
        existsQuery.setFieldName("field1");
        assertEquals(existsQuery.getFieldName(), "field1");
    }

    @Test
    public void testQueryType() {
        ExistsQuery existsQuery = new ExistsQuery();
        existsQuery.setFieldName("field1");
        assertEquals(existsQuery.getQueryType(), QueryType.QueryType_ExistsQuery);
    }

    @Test
    public void testSerialize() {
        ExistsQuery existsQuery = new ExistsQuery();
        existsQuery.setFieldName("field1");
        ByteString actural = existsQuery.serialize();

        Search.ExistsQuery.Builder builder = Search.ExistsQuery.newBuilder();
        builder.setFieldName("field1");
        ByteString expected = builder.build().toByteString();

        assertEquals(expected, actural);
    }

    @Test
    public void testSerializeFail() {
        ExistsQuery existsQuery = new ExistsQuery();
        try {
            existsQuery.serialize();
            fail();
        } catch (NullPointerException ignored) {
        }
    }
}
