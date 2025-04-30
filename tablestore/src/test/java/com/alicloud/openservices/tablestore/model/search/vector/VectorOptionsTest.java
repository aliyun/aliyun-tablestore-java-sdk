package com.alicloud.openservices.tablestore.model.search.vector;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.core.protocol.SearchProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.SearchProtocolParser;
import com.alicloud.openservices.tablestore.core.utils.Repeat;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

public class VectorOptionsTest extends BaseSearchTest {

    @Test
    public void testGetDataType() {
        VectorOptions options = new VectorOptions(VectorDataType.FLOAT_32, 12, VectorMetricType.COSINE);
        Assert.assertEquals(VectorDataType.FLOAT_32, options.getDataType());
    }

    @Test
    public void testGetDimension() {
        VectorOptions options = new VectorOptions(VectorDataType.FLOAT_32, 12, VectorMetricType.COSINE);
        Assert.assertEquals(12, options.getDimension().intValue());

    }

    @Test
    public void testGetMetricType() {
        VectorOptions options = new VectorOptions(VectorDataType.FLOAT_32, 12, VectorMetricType.COSINE);
        Assert.assertEquals(VectorMetricType.COSINE, options.getMetricType());

    }

    @Test
    @Repeat(1)
    public void testJsonize() {
        VectorOptions options = new VectorOptions(VectorDataType.FLOAT_32, 12, VectorMetricType.COSINE);
        System.out.println(options.jsonize());

        FieldSchema fieldSchema = new FieldSchema("testName", FieldType.VECTOR);
        fieldSchema.setVectorOptions(options);
        System.out.println(fieldSchema.jsonize());
    }

    @Test
    @Repeat(100)
    public void testParse() {
        Gson gson = new Gson();
        {
            VectorOptions options = new VectorOptions(VectorDataType.FLOAT_32, 12, VectorMetricType.COSINE);
            Search.VectorOptions pb = SearchProtocolBuilder.buildVectorOptions(options);
            VectorOptions vectorOptions = SearchProtocolParser.toVectorOptions(pb);
            Assert.assertEquals(gson.toJson(options), gson.toJson(vectorOptions));
        }
        {
            VectorOptions options = randomVectorOptions();
            Search.VectorOptions pb = SearchProtocolBuilder.buildVectorOptions(options);
            VectorOptions vectorOptions = SearchProtocolParser.toVectorOptions(pb);
            Assert.assertEquals(gson.toJson(options), gson.toJson(vectorOptions));
        }
    }
}