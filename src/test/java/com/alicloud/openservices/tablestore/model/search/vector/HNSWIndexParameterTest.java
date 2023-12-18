package com.alicloud.openservices.tablestore.model.search.vector;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.core.protocol.Search;
import org.junit.Assert;
import org.junit.Test;

public class HNSWIndexParameterTest extends BaseSearchTest {

    @Test
    public void testSerialize() {
        int m = random().nextInt();
        int ef = random().nextInt();
        HNSWIndexParameter hnswIndexParameter = new HNSWIndexParameter(m, ef);

        byte[] byteArray = Search.HNSWIndexParameter.newBuilder()
                .setM(m)
                .setEfConstruction(ef)
                .build().toByteArray();

        Assert.assertArrayEquals(byteArray, hnswIndexParameter.serialize().toByteArray());
    }

    @Test
    public void testJsonize() {
        int m = random().nextInt();
        int ef = random().nextInt();
        HNSWIndexParameter hnswIndexParameter = new HNSWIndexParameter(m, ef);
        System.out.println(hnswIndexParameter.jsonize());
    }
}