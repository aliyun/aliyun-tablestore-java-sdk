package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.google.protobuf.ByteString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KnnVectorQueryTest extends BaseSearchTest {

    @Test
    public void testGetFieldName() {
        KnnVectorQuery query = new KnnVectorQuery();
        query.setFieldName("field1");
        assertEquals(query.getFieldName(), "field1");
    }

    @Test
    public void testQueryType() {
        KnnVectorQuery query = new KnnVectorQuery();
        assertEquals(query.getQueryType(), QueryType.QueryType_KnnVectorQuery);
    }

    @Test
    public void testTopK() {
        KnnVectorQuery query = new KnnVectorQuery();
        query.setTopK(998);
        assertEquals(query.getTopK().intValue(), 998);
    }

    @Test
    public void testMinScore() {
        KnnVectorQuery query = new KnnVectorQuery();
        query.setMinScore(0.5f);
        assertEquals(query.getMinScore(), 0.5f, 0.00001);
    }

    @Test
    public void testNumCandidates() {
        KnnVectorQuery query = new KnnVectorQuery();
        query.setNumCandidates(100);
        assertEquals(query.getNumCandidates().intValue(), 100);
    }

    @Test
    public void testFloatVector() {
        float[] floats = new float[random().nextInt(100) + 1];
        for (int i = 0; i < floats.length; i++) {
            floats[i] = random().nextFloat();
        }
        KnnVectorQuery query = new KnnVectorQuery();
        query.setFloat32QueryVector(floats);
        assertEquals(query.getFloat32QueryVector(), floats);
    }

    @Test
    public void testSerialize() {
        KnnVectorQuery query = new KnnVectorQuery();
        query.setFieldName("field1");
        ByteString actual = query.serialize();

        Search.KnnVectorQuery.Builder builder = Search.KnnVectorQuery.newBuilder();
        builder.setFieldName("field1");
        ByteString expected = builder.build().toByteString();

        assertEquals(expected, actual);
    }

    @Test
    public void testBuilder() {
        KnnVectorQuery query = new KnnVectorQuery();
        String fieldName = randomString(3);
        query.setFieldName(fieldName);
        int topK = random().nextInt();
        query.setTopK(topK);
        float minScore = 0.5f;
        query.setMinScore(minScore);
        query.setNumCandidates(topK + 5);
        float[] floats = new float[random().nextInt(100) + 1];
        for (int i = 0; i < floats.length; i++) {
            floats[i] = random().nextFloat();
        }
        query.setFloat32QueryVector(floats);
        byte[] bytes = new byte[random().nextInt(100) + 1];
        random().nextBytes(bytes);
        Query filter = randomQuery();
        query.setFilter(filter);
        float weight = random().nextFloat();
        query.setWeight(weight);


        KnnVectorQuery.Builder builder = KnnVectorQuery.newBuilder()
                .field(fieldName)
                .topK(topK)
                .queryVector(floats)
                .filter(filter)
                .minScore(minScore)
                .numCandidates(topK + 5)
                .weight(weight);
        assertEquals(query.serialize(), builder.build().serialize());
    }
}