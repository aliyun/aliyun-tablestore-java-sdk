package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.ComputeSplitsRequest;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.SearchIndexSplitsOptions;
import com.alicloud.openservices.tablestore.model.StartLocalTransactionRequest;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder.buildComputeSplitsRequest;
import static com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder.buildStartLocalTransactionRequest;
import static org.junit.Assert.*;

public class OTSProtocolBuilderTest {

    @Test
    public void testBuildComputeSplitsRequest() {
        String tableName = "tableName";
        String indexName = "indexName";

        {
            ComputeSplitsRequest request = ComputeSplitsRequest.newBuilder()
                .tableName(tableName)
                .splitsOptions(new SearchIndexSplitsOptions(indexName))
                .build();
            OtsInternalApi.ComputeSplitsRequest request1 = buildComputeSplitsRequest(request);

            OtsInternalApi.ComputeSplitsRequest.Builder builder = OtsInternalApi.ComputeSplitsRequest.newBuilder();
            builder.setTableName(tableName);
            OtsInternalApi.SearchIndexSplitsOptions.Builder sBuilder = OtsInternalApi.SearchIndexSplitsOptions.newBuilder();
            builder.setSearchIndexSplitsOptions(sBuilder.setIndexName(indexName));
            OtsInternalApi.ComputeSplitsRequest request2 = builder.build();
            assertEquals(request1.toByteString(), request2.toByteString());
        }
        // tableName no set
        {
            ComputeSplitsRequest request = ComputeSplitsRequest.newBuilder()
                .splitsOptions(new SearchIndexSplitsOptions(indexName))
                .build();
            OtsInternalApi.ComputeSplitsRequest request1 = buildComputeSplitsRequest(request);

            OtsInternalApi.ComputeSplitsRequest.Builder builder = OtsInternalApi.ComputeSplitsRequest.newBuilder();
            OtsInternalApi.SearchIndexSplitsOptions.Builder sBuilder = OtsInternalApi.SearchIndexSplitsOptions.newBuilder();
            builder.setSearchIndexSplitsOptions(sBuilder.setIndexName(indexName));
            OtsInternalApi.ComputeSplitsRequest request2 = builder.build();
            assertEquals(request1.toByteString(), request2.toByteString());
        }
        // indexName no set
        {
            ComputeSplitsRequest request = ComputeSplitsRequest.newBuilder()
                .tableName(tableName)
                .splitsOptions(new SearchIndexSplitsOptions())
                .build();
            OtsInternalApi.ComputeSplitsRequest request1 = buildComputeSplitsRequest(request);

            OtsInternalApi.ComputeSplitsRequest.Builder builder = OtsInternalApi.ComputeSplitsRequest.newBuilder();
            builder.setTableName(tableName);
            OtsInternalApi.SearchIndexSplitsOptions.Builder sBuilder = OtsInternalApi.SearchIndexSplitsOptions.newBuilder();
            builder.setSearchIndexSplitsOptions(sBuilder);
            OtsInternalApi.ComputeSplitsRequest request2 = builder.build();
            assertEquals(request1.toByteString(), request2.toByteString());
        }
        // SearchIndexSplitsOptions no set
        {
            ComputeSplitsRequest request = ComputeSplitsRequest.newBuilder()
                .tableName(tableName)
                .build();
            OtsInternalApi.ComputeSplitsRequest request1 = buildComputeSplitsRequest(request);

            OtsInternalApi.ComputeSplitsRequest.Builder builder = OtsInternalApi.ComputeSplitsRequest.newBuilder();
            builder.setTableName(tableName);
            OtsInternalApi.ComputeSplitsRequest request2 = builder.build();
            assertEquals(request1.toByteString(), request2.toByteString());
        }
        // all no set
        {
            ComputeSplitsRequest request = ComputeSplitsRequest.newBuilder()
                .build();
            OtsInternalApi.ComputeSplitsRequest request1 = buildComputeSplitsRequest(request);

            OtsInternalApi.ComputeSplitsRequest.Builder builder = OtsInternalApi.ComputeSplitsRequest.newBuilder();
            OtsInternalApi.ComputeSplitsRequest request2 = builder.build();
            assertEquals(request1.toByteString(), request2.toByteString());
        }

    }

    @Test
    public void testBuildStartLocalTransactionRequestWithoutRowKeys() throws IOException {
        String tableName = "testTable";

        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("partitionKey"))
            .build();

        StartLocalTransactionRequest request = new StartLocalTransactionRequest(tableName, primaryKey);
        OtsInternalApi.StartLocalTransactionRequest pbRequest = buildStartLocalTransactionRequest(request);

        OtsInternalApi.StartLocalTransactionRequest.Builder expectedBuilder =
            OtsInternalApi.StartLocalTransactionRequest.newBuilder();
        expectedBuilder.setTableName(tableName);
        expectedBuilder.setKey(com.aliyun.ots.thirdparty.com.google.protobuf.ByteString.copyFrom(
            PlainBufferBuilder.buildPrimaryKeyWithHeader(primaryKey)));
        OtsInternalApi.StartLocalTransactionRequest expectedPbRequest = expectedBuilder.build();

        assertEquals(expectedPbRequest.toByteString(), pbRequest.toByteString());
        assertEquals(0, pbRequest.getRowKeysCount());
    }

    @Test
    public void testBuildStartLocalTransactionRequestWithSingleRowKey() throws IOException {
        String tableName = "testTable";

        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("partitionKey"))
            .build();

        PrimaryKey rowKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("partitionKey"))
            .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(100L))
            .build();

        StartLocalTransactionRequest request = new StartLocalTransactionRequest(
            tableName, primaryKey, Collections.singletonList(rowKey));
        OtsInternalApi.StartLocalTransactionRequest pbRequest = buildStartLocalTransactionRequest(request);

        OtsInternalApi.StartLocalTransactionRequest.Builder expectedBuilder =
            OtsInternalApi.StartLocalTransactionRequest.newBuilder();
        expectedBuilder.setTableName(tableName);
        expectedBuilder.setKey(com.aliyun.ots.thirdparty.com.google.protobuf.ByteString.copyFrom(
            PlainBufferBuilder.buildPrimaryKeyWithHeader(primaryKey)));
        expectedBuilder.addRowKeys(com.aliyun.ots.thirdparty.com.google.protobuf.ByteString.copyFrom(
            PlainBufferBuilder.buildPrimaryKeyWithHeader(rowKey)));
        OtsInternalApi.StartLocalTransactionRequest expectedPbRequest = expectedBuilder.build();

        assertEquals(expectedPbRequest.toByteString(), pbRequest.toByteString());
        assertEquals(1, pbRequest.getRowKeysCount());
    }

    @Test
    public void testBuildStartLocalTransactionRequestWithMultipleRowKeys() throws IOException {
        String tableName = "testTable";

        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("partitionKey"))
            .build();

        PrimaryKey rowKey1 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("partitionKey"))
            .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(100L))
            .build();

        PrimaryKey rowKey2 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("partitionKey"))
            .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(200L))
            .build();

        PrimaryKey rowKey3 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("partitionKey"))
            .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(300L))
            .build();

        StartLocalTransactionRequest request = new StartLocalTransactionRequest(
            tableName, primaryKey, Arrays.asList(rowKey1, rowKey2, rowKey3));
        OtsInternalApi.StartLocalTransactionRequest pbRequest = buildStartLocalTransactionRequest(request);

        OtsInternalApi.StartLocalTransactionRequest.Builder expectedBuilder =
            OtsInternalApi.StartLocalTransactionRequest.newBuilder();
        expectedBuilder.setTableName(tableName);
        expectedBuilder.setKey(com.aliyun.ots.thirdparty.com.google.protobuf.ByteString.copyFrom(
            PlainBufferBuilder.buildPrimaryKeyWithHeader(primaryKey)));
        expectedBuilder.addRowKeys(com.aliyun.ots.thirdparty.com.google.protobuf.ByteString.copyFrom(
            PlainBufferBuilder.buildPrimaryKeyWithHeader(rowKey1)));
        expectedBuilder.addRowKeys(com.aliyun.ots.thirdparty.com.google.protobuf.ByteString.copyFrom(
            PlainBufferBuilder.buildPrimaryKeyWithHeader(rowKey2)));
        expectedBuilder.addRowKeys(com.aliyun.ots.thirdparty.com.google.protobuf.ByteString.copyFrom(
            PlainBufferBuilder.buildPrimaryKeyWithHeader(rowKey3)));
        OtsInternalApi.StartLocalTransactionRequest expectedPbRequest = expectedBuilder.build();

        assertEquals(expectedPbRequest.toByteString(), pbRequest.toByteString());
        assertEquals(3, pbRequest.getRowKeysCount());
    }

    @Test
    public void testBuildStartLocalTransactionRequestRowKeysOrder() throws IOException {
        String tableName = "testTable";

        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("partitionKey"))
            .build();

        PrimaryKey rowKey1 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("partitionKey"))
            .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(1L))
            .build();

        PrimaryKey rowKey2 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("partitionKey"))
            .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(2L))
            .build();

        // add rowKey2 first, then rowKey1 — order must be preserved
        StartLocalTransactionRequest request = new StartLocalTransactionRequest(tableName, primaryKey);
        request.addRowKey(rowKey2);
        request.addRowKey(rowKey1);

        OtsInternalApi.StartLocalTransactionRequest pbRequest = buildStartLocalTransactionRequest(request);

        assertEquals(2, pbRequest.getRowKeysCount());
        assertEquals(
            com.aliyun.ots.thirdparty.com.google.protobuf.ByteString.copyFrom(
                PlainBufferBuilder.buildPrimaryKeyWithHeader(rowKey2)),
            pbRequest.getRowKeys(0));
        assertEquals(
            com.aliyun.ots.thirdparty.com.google.protobuf.ByteString.copyFrom(
                PlainBufferBuilder.buildPrimaryKeyWithHeader(rowKey1)),
            pbRequest.getRowKeys(1));
    }

}