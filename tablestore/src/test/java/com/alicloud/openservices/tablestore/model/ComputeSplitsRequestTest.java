package com.alicloud.openservices.tablestore.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class ComputeSplitsRequestTest {

    static Gson gson = new GsonBuilder()
        .disableHtmlEscaping()
        .disableInnerClassSerialization()
        .serializeNulls()
        .serializeSpecialFloatingPointValues()
        .enableComplexMapKeySerialization().create();

    @Test
    public void getOperationName() {
        ComputeSplitsRequest computeSplitsRequest = new ComputeSplitsRequest();
        assertEquals(computeSplitsRequest.getOperationName(), OperationNames.OP_COMPUTE_SPLITS);
    }

    @Test
    public void getTableName() {
        ComputeSplitsRequest computeSplitsRequest1 = new ComputeSplitsRequest();
        computeSplitsRequest1.setTableName("123ffff");
        ComputeSplitsRequest computeSplitsRequest2 = ComputeSplitsRequest.newBuilder().tableName("123ffff").build();
        Assert.assertEquals(computeSplitsRequest2.getTableName(),computeSplitsRequest1.getTableName());
        Assert.assertEquals(gson.toJson(computeSplitsRequest2),gson.toJson(computeSplitsRequest1) );
    }

    @Test
    public void setTableName() {
        ComputeSplitsRequest computeSplitsRequest1 = new ComputeSplitsRequest();
        computeSplitsRequest1.setTableName("test1");
        ComputeSplitsRequest computeSplitsRequest2 = ComputeSplitsRequest.newBuilder().tableName("test1").build();
        Assert.assertEquals(gson.toJson(computeSplitsRequest2),gson.toJson(computeSplitsRequest1) );
    }

    @Test
    public void getOptions() {
        ComputeSplitsRequest computeSplitsRequest1 = new ComputeSplitsRequest();
        computeSplitsRequest1.setSplitsOptions(new SearchIndexSplitsOptions("test1"));
        ComputeSplitsRequest computeSplitsRequest2 = ComputeSplitsRequest.newBuilder().splitsOptions(new SearchIndexSplitsOptions("test1")).build();
        Assert.assertEquals(gson.toJson(computeSplitsRequest2),gson.toJson(computeSplitsRequest1) );
        Assert.assertEquals(computeSplitsRequest2.getSearchIndexSplitsOptions().getIndexName(),computeSplitsRequest1.getSearchIndexSplitsOptions().getIndexName());
    }

    @Test
    public void setOptions() {
        ComputeSplitsRequest computeSplitsRequest1 = new ComputeSplitsRequest();
        computeSplitsRequest1.setSplitsOptions(new SearchIndexSplitsOptions("test1"));
        ComputeSplitsRequest computeSplitsRequest2 = ComputeSplitsRequest.newBuilder().splitsOptions(new SearchIndexSplitsOptions("test1")).build();
        Assert.assertEquals(gson.toJson(computeSplitsRequest2),gson.toJson(computeSplitsRequest1) );
    }

    @Test
    public void newBuilder() {
        ComputeSplitsRequest computeSplitsRequest1 = new ComputeSplitsRequest();
        computeSplitsRequest1.setSplitsOptions(new SearchIndexSplitsOptions("test1"));
        computeSplitsRequest1.setTableName("ttt111");
        ComputeSplitsRequest computeSplitsRequest2 = ComputeSplitsRequest
            .newBuilder()
            .tableName("ttt111")
            .splitsOptions(new SearchIndexSplitsOptions("test1"))
            .build();
        Assert.assertEquals(gson.toJson(computeSplitsRequest2),gson.toJson(computeSplitsRequest1) );
    }
}