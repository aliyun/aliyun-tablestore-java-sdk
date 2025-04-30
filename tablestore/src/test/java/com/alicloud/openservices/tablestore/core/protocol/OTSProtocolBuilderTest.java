package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.ComputeSplitsRequest;
import com.alicloud.openservices.tablestore.model.SearchIndexSplitsOptions;
import org.junit.Test;

import static com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder.buildComputeSplitsRequest;
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

}