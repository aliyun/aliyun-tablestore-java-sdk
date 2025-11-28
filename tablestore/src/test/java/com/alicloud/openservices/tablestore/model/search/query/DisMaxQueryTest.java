package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;
import org.junit.Test;

import java.util.List;

import static com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder.buildQuery;
import static org.junit.Assert.assertEquals;

public class DisMaxQueryTest extends BaseSearchTest {
    @Test
    public void testQueryType() {
        DisMaxQuery query = new DisMaxQuery();
        assertEquals(QueryType.QueryType_DisMaxQuery, query.getQueryType());
    }

    @Test
    public void testSerialize() {
        DisMaxQuery query = new DisMaxQuery();
        List<Query> queries = randomQueries();
        query.setQueries(queries);
        query.setTieBreaker(0.5f);
        query.setWeight(0.5f);
        ByteString actual = query.serialize();

        Search.DisMaxQuery.Builder builder = Search.DisMaxQuery.newBuilder();
        builder.setTieBreaker(0.5f);
        builder.setWeight(0.5f);
        for (Query q : query.getQueries()) {
            builder.addQueries(buildQuery(q));
        }
        ByteString expected = builder.build().toByteString();
        assertEquals(expected, actual);
    }

    @Test
    public void testTieBreaker() {
        DisMaxQuery query = new DisMaxQuery();
        query.setTieBreaker(0.5f);
        assertEquals(0.5f, query.getTieBreaker(), 0.0001);
    }

    @Test
    public void testWeight() {
        DisMaxQuery query = new DisMaxQuery();
        query.setWeight(0.5f);
        assertEquals(0.5f, query.getWeight(), 0.0001);
    }
}
