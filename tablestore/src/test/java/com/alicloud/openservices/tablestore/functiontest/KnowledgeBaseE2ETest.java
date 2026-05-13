package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.model.knowledgebase.*;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/**
 * E2E tests for Knowledge Base functionality. Based on Python SDK test coverage from agent-storage-e2e-test/tests/api_tests
 */
public class KnowledgeBaseE2ETest {

    private SyncClient client;
    private List<String> createdKnowledgeBases = new ArrayList<>();
    private static final String KB_NAME_PREFIX = "test_kb_java_";

    @Before
    public void setUp() {
        Assume.assumeTrue(!Utils.useGlobalTxn());
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();
        final String instanceName = settings.getOTSInstanceName();

        client = new SyncClient(endPoint, accessId, accessKey, instanceName);
        createdKnowledgeBases = new ArrayList<>();
    }

    @After
    public void tearDown() {
        // Clean up all created knowledge bases
        for (String kbName : createdKnowledgeBases) {
            try {
                DeleteKnowledgeBaseRequest deleteRequest = new DeleteKnowledgeBaseRequest();
                deleteRequest.setKnowledgeBaseName(kbName);
                client.deleteKnowledgeBase(deleteRequest);
            } catch (Exception e) {
                // Ignore cleanup errors
                System.err.println("Failed to cleanup knowledge base: " + kbName + ", error: " + e.getMessage());
            }
        }

        if (client != null) {
            client.shutdown();
        }
    }

    private String generateKbName() {
        return KB_NAME_PREFIX + UUID.randomUUID().toString().substring(0, 8);
    }

    // ============================================================================
    // Create Knowledge Base Tests
    // ============================================================================

    @Test
    public void testCreateBasicKnowledgeBase() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName);

        CreateKnowledgeBaseResponse response = client.createKnowledgeBase(request);
        createdKnowledgeBases.add(kbName);

        assertEquals("SUCCESS", response.getCode());
        assertEquals("succeed", response.getMessage());
    }

    @Test
    public void testCreateKnowledgeBaseWithDescription() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName);
        request.setDescription("Test knowledge base with description");

        CreateKnowledgeBaseResponse response = client.createKnowledgeBase(request);
        createdKnowledgeBases.add(kbName);

        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testCreateKnowledgeBaseWithSubspaceTrue() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName);
        request.setSubspace(true);

        CreateKnowledgeBaseResponse response = client.createKnowledgeBase(request);
        createdKnowledgeBases.add(kbName);

        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testCreateKnowledgeBaseWithSubspaceFalse() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName);
        request.setSubspace(false);

        CreateKnowledgeBaseResponse response = client.createKnowledgeBase(request);
        createdKnowledgeBases.add(kbName);

        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testCreateKnowledgeBaseWithTags() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName);
        request.setTags(Arrays.asList("tag1", "tag2", "tag3"));

        CreateKnowledgeBaseResponse response = client.createKnowledgeBase(request);
        createdKnowledgeBases.add(kbName);

        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testCreateKnowledgeBaseWithMetadata() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName);

        List<MetadataField> metadata = new ArrayList<>();
        metadata.add(new MetadataField("author", "string"));
        metadata.add(new MetadataField("year", "long"));
        metadata.add(new MetadataField("score", "double"));
        request.setMetadata(metadata);

        CreateKnowledgeBaseResponse response = client.createKnowledgeBase(request);
        createdKnowledgeBases.add(kbName);

        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testCreateKnowledgeBaseWithAllParameters() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName);
        request.setDescription("Complete knowledge base");
        request.setSubspace(true);
        request.setTags(Arrays.asList("comprehensive", "test"));

        List<MetadataField> metadata = new ArrayList<>();
        metadata.add(new MetadataField("category", "string"));
        metadata.add(new MetadataField("priority", "long"));
        request.setMetadata(metadata);

        CreateKnowledgeBaseResponse response = client.createKnowledgeBase(request);
        createdKnowledgeBases.add(kbName);

        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testCreateKnowledgeBaseNameMinLength() {
        String kbName = "a"; // 1 character
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName);

        CreateKnowledgeBaseResponse response = client.createKnowledgeBase(request);
        createdKnowledgeBases.add(kbName);

        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testCreateKnowledgeBaseNameMaxLength() {
        // 64 characters
        StringBuilder kbName = new StringBuilder();
        for (int i = 0; i < 64; i++) {
            kbName.append("a");
        }
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName.toString());

        CreateKnowledgeBaseResponse response = client.createKnowledgeBase(request);
        createdKnowledgeBases.add(kbName.toString());

        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testCreateKnowledgeBaseNameWithUnderscore() {
        String kbName = "test_kb_with_underscore";
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName);

        CreateKnowledgeBaseResponse response = client.createKnowledgeBase(request);
        createdKnowledgeBases.add(kbName);

        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testCreateKnowledgeBaseNameWithNumbers() {
        String kbName = "test123kb456";
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName);

        CreateKnowledgeBaseResponse response = client.createKnowledgeBase(request);
        createdKnowledgeBases.add(kbName);

        assertEquals("SUCCESS", response.getCode());
    }

    @Test(expected = Exception.class)
    public void testCreateKnowledgeBaseDuplicateName() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName);

        client.createKnowledgeBase(request);
        createdKnowledgeBases.add(kbName);

        // Try to create again with same name - should fail
        client.createKnowledgeBase(request);
    }

    // ============================================================================
    // Delete Knowledge Base Tests
    // ============================================================================

    @Test
    public void testDeleteExistingKnowledgeBase() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest createRequest = new CreateKnowledgeBaseRequest(kbName);
        client.createKnowledgeBase(createRequest);

        DeleteKnowledgeBaseRequest deleteRequest = new DeleteKnowledgeBaseRequest();
        deleteRequest.setKnowledgeBaseName(kbName);

        DeleteKnowledgeBaseResponse response = client.deleteKnowledgeBase(deleteRequest);
        assertEquals("SUCCESS", response.getCode());

        // Remove from cleanup list since already deleted
        createdKnowledgeBases.remove(kbName);
    }

    @Test
    public void testDeleteKnowledgeBaseWithAllParameters() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest createRequest = new CreateKnowledgeBaseRequest(kbName);
        createRequest.setDescription("KB to delete");
        createRequest.setSubspace(true);
        createRequest.setTags(Arrays.asList("delete", "test"));
        client.createKnowledgeBase(createRequest);

        DeleteKnowledgeBaseRequest deleteRequest = new DeleteKnowledgeBaseRequest();
        deleteRequest.setKnowledgeBaseName(kbName);

        DeleteKnowledgeBaseResponse response = client.deleteKnowledgeBase(deleteRequest);
        assertEquals("SUCCESS", response.getCode());

        createdKnowledgeBases.remove(kbName);
    }

    @Test(expected = Exception.class)
    public void testDeleteNonExistentKnowledgeBase() {
        DeleteKnowledgeBaseRequest deleteRequest = new DeleteKnowledgeBaseRequest();
        deleteRequest.setKnowledgeBaseName("non_existent_kb_" + UUID.randomUUID());

        client.deleteKnowledgeBase(deleteRequest);
    }

    @Test(expected = Exception.class)
    public void testDeleteAlreadyDeletedKnowledgeBase() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest createRequest = new CreateKnowledgeBaseRequest(kbName);
        client.createKnowledgeBase(createRequest);

        DeleteKnowledgeBaseRequest deleteRequest = new DeleteKnowledgeBaseRequest();
        deleteRequest.setKnowledgeBaseName(kbName);

        client.deleteKnowledgeBase(deleteRequest);
        // Try to delete again - should fail
        client.deleteKnowledgeBase(deleteRequest);
    }

    // ============================================================================
    // Describe Knowledge Base Tests
    // ============================================================================

    @Test
    public void testDescribeExistingKnowledgeBase() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest createRequest = new CreateKnowledgeBaseRequest(kbName);
        client.createKnowledgeBase(createRequest);
        createdKnowledgeBases.add(kbName);

        DescribeKnowledgeBaseRequest describeRequest = new DescribeKnowledgeBaseRequest();
        describeRequest.setKnowledgeBaseName(kbName);

        DescribeKnowledgeBaseResponse response = client.describeKnowledgeBase(describeRequest);
        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData());
        assertEquals(kbName, response.getData().getKnowledgeBaseName());
    }

    @Test
    public void testDescribeKnowledgeBaseWithDescription() {
        String kbName = generateKbName();
        String description = "Test description";
        CreateKnowledgeBaseRequest createRequest = new CreateKnowledgeBaseRequest(kbName);
        createRequest.setDescription(description);
        client.createKnowledgeBase(createRequest);
        createdKnowledgeBases.add(kbName);

        DescribeKnowledgeBaseRequest describeRequest = new DescribeKnowledgeBaseRequest();
        describeRequest.setKnowledgeBaseName(kbName);

        DescribeKnowledgeBaseResponse response = client.describeKnowledgeBase(describeRequest);
        assertEquals("SUCCESS", response.getCode());
        assertEquals(description, response.getData().getDescription());
    }

    @Test
    public void testDescribeKnowledgeBaseWithSubspace() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest createRequest = new CreateKnowledgeBaseRequest(kbName);
        createRequest.setSubspace(true);
        client.createKnowledgeBase(createRequest);
        createdKnowledgeBases.add(kbName);

        DescribeKnowledgeBaseRequest describeRequest = new DescribeKnowledgeBaseRequest();
        describeRequest.setKnowledgeBaseName(kbName);

        DescribeKnowledgeBaseResponse response = client.describeKnowledgeBase(describeRequest);
        assertEquals("SUCCESS", response.getCode());
        assertTrue(response.getData().getSubspace());
    }

    @Test
    public void testDescribeKnowledgeBaseWithTags() {
        String kbName = generateKbName();
        List<String> tags = Arrays.asList("tag1", "tag2");
        CreateKnowledgeBaseRequest createRequest = new CreateKnowledgeBaseRequest(kbName);
        createRequest.setTags(tags);
        client.createKnowledgeBase(createRequest);
        createdKnowledgeBases.add(kbName);

        DescribeKnowledgeBaseRequest describeRequest = new DescribeKnowledgeBaseRequest();
        describeRequest.setKnowledgeBaseName(kbName);

        DescribeKnowledgeBaseResponse response = client.describeKnowledgeBase(describeRequest);
        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData().getTags());
        assertEquals(tags.size(), response.getData().getTags().size());
    }

    @Test
    public void testDescribeKnowledgeBaseResponseContainsTimestamps() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest createRequest = new CreateKnowledgeBaseRequest(kbName);
        client.createKnowledgeBase(createRequest);
        createdKnowledgeBases.add(kbName);

        DescribeKnowledgeBaseRequest describeRequest = new DescribeKnowledgeBaseRequest();
        describeRequest.setKnowledgeBaseName(kbName);

        DescribeKnowledgeBaseResponse response = client.describeKnowledgeBase(describeRequest);
        assertEquals("SUCCESS", response.getCode());
        assertTrue(response.getData().getCreatedAt() > 0);
        assertTrue(response.getData().getUpdatedAt() > 0);
    }

    @Test(expected = Exception.class)
    public void testDescribeNonExistentKnowledgeBase() {
        DescribeKnowledgeBaseRequest describeRequest = new DescribeKnowledgeBaseRequest();
        describeRequest.setKnowledgeBaseName("non_existent_kb_" + UUID.randomUUID());

        client.describeKnowledgeBase(describeRequest);
    }

    // ============================================================================
    // List Knowledge Base Tests
    // ============================================================================

    @Test
    public void testListKnowledgeBaseEmpty() {
        ListKnowledgeBaseRequest request = new ListKnowledgeBaseRequest();

        ListKnowledgeBaseResponse response = client.listKnowledgeBase(request);
        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData());
    }

    @Test
    public void testListKnowledgeBaseWithMaxResults() {
        // Create multiple knowledge bases
        for (int i = 0; i < 3; i++) {
            String kbName = generateKbName();
            CreateKnowledgeBaseRequest createRequest = new CreateKnowledgeBaseRequest(kbName);
            client.createKnowledgeBase(createRequest);
            createdKnowledgeBases.add(kbName);
        }

        ListKnowledgeBaseRequest request = new ListKnowledgeBaseRequest();
        request.setMaxResults(2);

        ListKnowledgeBaseResponse response = client.listKnowledgeBase(request);
        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData().getKnowledgeBases());
        assertTrue(response.getData().getKnowledgeBases().size() <= 2);
    }

    @Test
    public void testListKnowledgeBaseWithPagination() {
        // Create multiple knowledge bases
        for (int i = 0; i < 5; i++) {
            String kbName = generateKbName();
            CreateKnowledgeBaseRequest createRequest = new CreateKnowledgeBaseRequest(kbName);
            client.createKnowledgeBase(createRequest);
            createdKnowledgeBases.add(kbName);
        }

        ListKnowledgeBaseRequest request = new ListKnowledgeBaseRequest();
        request.setMaxResults(2);

        List<ListKnowledgeBaseResponse.KnowledgeBaseInfo> allKbs = new ArrayList<>();
        String nextToken = null;

        do {
            if (nextToken != null) {
                request.setNextToken(nextToken);
            }

            ListKnowledgeBaseResponse response = client.listKnowledgeBase(request);
            assertEquals("SUCCESS", response.getCode());

            if (response.getData().getKnowledgeBases() != null) {
                allKbs.addAll(response.getData().getKnowledgeBases());
            }

            nextToken = response.getData().getNextToken();
        } while (nextToken != null && !nextToken.isEmpty());

        assertTrue(allKbs.size() >= 5);
    }

    @Test
    public void testListKnowledgeBaseResponseContainsExpectedFields() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest createRequest = new CreateKnowledgeBaseRequest(kbName);
        createRequest.setDescription("Test KB");
        createRequest.setTags(Arrays.asList("test"));
        client.createKnowledgeBase(createRequest);
        createdKnowledgeBases.add(kbName);

        ListKnowledgeBaseRequest request = new ListKnowledgeBaseRequest();
        ListKnowledgeBaseResponse response = client.listKnowledgeBase(request);

        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData().getKnowledgeBases());

        // Find our created KB
        boolean found = false;
        for (ListKnowledgeBaseResponse.KnowledgeBaseInfo kb : response.getData().getKnowledgeBases()) {
            if (kbName.equals(kb.getKnowledgeBaseName())) {
                found = true;
                assertNotNull(kb.getCreatedAt());
                assertNotNull(kb.getUpdatedAt());
                break;
            }
        }
        assertTrue("Created knowledge base should be in list", found);
    }

}
