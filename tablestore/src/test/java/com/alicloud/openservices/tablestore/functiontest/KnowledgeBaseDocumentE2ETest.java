package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.common.OTSHelper;
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
 * E2E tests for Knowledge Base Document functionality. Tests for AddDocuments, DeleteDocuments, GetDocument, and ListDocuments operations.
 */
public class KnowledgeBaseDocumentE2ETest {

    private SyncClient client;
    private List<String> createdKnowledgeBases = new ArrayList<>();
    private static final String KB_NAME_PREFIX = "test_kb_doc_";
    private static final String TEST_OSS_KEY = "oss://non_existent_bucket/test.pdf";

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

    private String createKnowledgeBaseWithSubspace() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName);
        request.setSubspace(true);

        List<MetadataField> metadata = new ArrayList<>();
        metadata.add(new MetadataField("author", "string"));
        metadata.add(new MetadataField("year", "long"));
        request.setMetadata(metadata);

        OTSHelper.createKnowledgeBaseWithRetry(client, request);
        createdKnowledgeBases.add(kbName);
        return kbName;
    }

    private String createKnowledgeBaseWithoutSubspace() {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName);
        request.setSubspace(false);

        OTSHelper.createKnowledgeBaseWithRetry(client, request);
        createdKnowledgeBases.add(kbName);
        return kbName;
    }

    // ============================================================================
    // AddDocuments Tests
    // ============================================================================

    @Test(expected = Exception.class)
    public void testAddDocumentsToNonExistentKb() {
        String kbName = "non_existent_kb_" + UUID.randomUUID();
        AddDocumentsRequest request = new AddDocumentsRequest(kbName);

        List<AddDocumentsRequest.DocumentItem> documents = new ArrayList<>();
        documents.add(new AddDocumentsRequest.DocumentItem(TEST_OSS_KEY));
        request.setDocuments(documents);

        client.addDocuments(request);
    }

    @Test(expected = Exception.class)
    public void testAddDocumentsWithoutSubspaceWhenRequired() {
        String kbName = createKnowledgeBaseWithSubspace();
        AddDocumentsRequest request = new AddDocumentsRequest(kbName);

        List<AddDocumentsRequest.DocumentItem> documents = new ArrayList<>();
        documents.add(new AddDocumentsRequest.DocumentItem(TEST_OSS_KEY));
        request.setDocuments(documents);

        client.addDocuments(request);
    }

    @Test(expected = Exception.class)
    public void testAddDocumentsWithEmptyDocumentsList() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        AddDocumentsRequest request = new AddDocumentsRequest(kbName);
        request.setDocuments(new ArrayList<AddDocumentsRequest.DocumentItem>());

        client.addDocuments(request);
    }

    @Test(expected = Exception.class)
    public void testAddDocumentsWithSubspaceWhenNotConfigured() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        AddDocumentsRequest request = new AddDocumentsRequest(kbName);
        request.setSubspace("test_subspace");

        List<AddDocumentsRequest.DocumentItem> documents = new ArrayList<>();
        documents.add(new AddDocumentsRequest.DocumentItem(TEST_OSS_KEY));
        request.setDocuments(documents);

        client.addDocuments(request);
    }

    @Test
    public void testAddDocumentsWithNonExistentFile() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        AddDocumentsRequest request = new AddDocumentsRequest(kbName);

        List<AddDocumentsRequest.DocumentItem> documents = new ArrayList<>();
        documents.add(new AddDocumentsRequest.DocumentItem(TEST_OSS_KEY));
        request.setDocuments(documents);

        AddDocumentsResponse response = client.addDocuments(request);
        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData());
        assertNotNull(response.getData().getDocumentDetails());
        assertFalse(response.getData().getDocumentDetails().isEmpty());

        // Document status should be failed since OSS file doesn't exist
        AddDocumentsResponse.DocumentDetail detail = response.getData().getDocumentDetails().get(0);
        assertEquals("failed", detail.getStatus());
    }

    @Test(expected = Exception.class)
    public void testAddDocumentsWithoutOssPath() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        AddDocumentsRequest request = new AddDocumentsRequest(kbName);

        List<AddDocumentsRequest.DocumentItem> documents = new ArrayList<>();
        documents.add(new AddDocumentsRequest.DocumentItem(null));
        request.setDocuments(documents);

        client.addDocuments(request);
    }

    // ============================================================================
    // DeleteDocuments Tests
    // ============================================================================

    @Test
    public void testDeleteDocumentsByOssKey() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        AddDocumentsRequest addRequest = new AddDocumentsRequest(kbName);

        List<AddDocumentsRequest.DocumentItem> documents = new ArrayList<>();
        documents.add(new AddDocumentsRequest.DocumentItem(TEST_OSS_KEY));
        addRequest.setDocuments(documents);

        AddDocumentsResponse addResponse = client.addDocuments(addRequest);
        String docId = addResponse.getData().getDocumentDetails().get(0).getDocId();

        // Delete the document
        DeleteDocumentsRequest deleteRequest = new DeleteDocumentsRequest(kbName);

        List<DeleteDocumentsRequest.DeleteDocumentItem> deleteItems = new ArrayList<>();
        DeleteDocumentsRequest.DeleteDocumentItem deleteItem = new DeleteDocumentsRequest.DeleteDocumentItem();
        deleteItem.setOssKey(TEST_OSS_KEY);
        deleteItems.add(deleteItem);
        deleteRequest.setDocuments(deleteItems);

        DeleteDocumentsResponse response = client.deleteDocuments(deleteRequest);
        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData());
        assertNotNull(response.getData().getDocumentDetails());
    }

    @Test
    public void testDeleteNonExistentDocument() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        DeleteDocumentsRequest request = new DeleteDocumentsRequest(kbName);

        List<DeleteDocumentsRequest.DeleteDocumentItem> documents = new ArrayList<>();
        DeleteDocumentsRequest.DeleteDocumentItem deleteItem = new DeleteDocumentsRequest.DeleteDocumentItem();
        deleteItem.setOssKey("oss://non_existent_bucket/non_existent_file.pdf");
        documents.add(deleteItem);
        request.setDocuments(documents);

        DeleteDocumentsResponse response = client.deleteDocuments(request);
        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData());
        assertNotNull(response.getData().getDocumentDetails());

        // Document status should be failed since document doesn't exist
        DeleteDocumentsResponse.DocumentDetail detail = response.getData().getDocumentDetails().get(0);
        assertEquals("failed", detail.getStatus());
    }

    @Test(expected = Exception.class)
    public void testDeleteDocumentsWithEmptyDocumentsList() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        DeleteDocumentsRequest request = new DeleteDocumentsRequest(kbName);
        request.setDocuments(new ArrayList<DeleteDocumentsRequest.DeleteDocumentItem>());

        client.deleteDocuments(request);
    }

    @Test(expected = Exception.class)
    public void testDeleteDocumentsWithoutSubspaceWhenRequired() {
        String kbName = createKnowledgeBaseWithSubspace();
        DeleteDocumentsRequest request = new DeleteDocumentsRequest(kbName);

        List<DeleteDocumentsRequest.DeleteDocumentItem> documents = new ArrayList<>();
        DeleteDocumentsRequest.DeleteDocumentItem deleteItem = new DeleteDocumentsRequest.DeleteDocumentItem();
        deleteItem.setOssKey(TEST_OSS_KEY);
        documents.add(deleteItem);
        request.setDocuments(documents);

        client.deleteDocuments(request);
    }

    @Test(expected = Exception.class)
    public void testDeleteDocumentsFromNonExistentKb() {
        String kbName = "non_existent_kb_" + UUID.randomUUID();
        DeleteDocumentsRequest request = new DeleteDocumentsRequest(kbName);

        List<DeleteDocumentsRequest.DeleteDocumentItem> documents = new ArrayList<>();
        DeleteDocumentsRequest.DeleteDocumentItem deleteItem = new DeleteDocumentsRequest.DeleteDocumentItem();
        deleteItem.setOssKey(TEST_OSS_KEY);
        documents.add(deleteItem);
        request.setDocuments(documents);

        client.deleteDocuments(request);
    }

    @Test
    public void testDeleteDocumentsWithWrongSubspace() {
        String kbName = createKnowledgeBaseWithSubspace();
        DeleteDocumentsRequest request = new DeleteDocumentsRequest(kbName);
        request.setSubspace("wrong_subspace");

        List<DeleteDocumentsRequest.DeleteDocumentItem> documents = new ArrayList<>();
        DeleteDocumentsRequest.DeleteDocumentItem deleteItem = new DeleteDocumentsRequest.DeleteDocumentItem();
        deleteItem.setOssKey(TEST_OSS_KEY);
        documents.add(deleteItem);
        request.setDocuments(documents);

        DeleteDocumentsResponse response = client.deleteDocuments(request);
        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData());
        assertNotNull(response.getData().getDocumentDetails());

        // Document status should be failed since subspace is wrong
        DeleteDocumentsResponse.DocumentDetail detail = response.getData().getDocumentDetails().get(0);
        assertEquals("failed", detail.getStatus());
    }

    @Test
    public void testDeleteDocumentsWithEmptyDocId() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        DeleteDocumentsRequest request = new DeleteDocumentsRequest(kbName);

        List<DeleteDocumentsRequest.DeleteDocumentItem> documents = new ArrayList<>();
        DeleteDocumentsRequest.DeleteDocumentItem deleteItem = new DeleteDocumentsRequest.DeleteDocumentItem();
        deleteItem.setDocId("");
        deleteItem.setOssKey(TEST_OSS_KEY);
        documents.add(deleteItem);
        request.setDocuments(documents);

        DeleteDocumentsResponse response = client.deleteDocuments(request);
        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData());
        assertNotNull(response.getData().getDocumentDetails());

        // Document status should be failed since docId is empty
        DeleteDocumentsResponse.DocumentDetail detail = response.getData().getDocumentDetails().get(0);
        assertEquals("failed", detail.getStatus());
    }

    // ============================================================================
    // GetDocument Tests
    // ============================================================================

    @Test
    public void testGetDocumentByOssKey() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        AddDocumentsRequest addRequest = new AddDocumentsRequest(kbName);

        List<AddDocumentsRequest.DocumentItem> documents = new ArrayList<>();
        documents.add(new AddDocumentsRequest.DocumentItem(TEST_OSS_KEY));
        addRequest.setDocuments(documents);

        client.addDocuments(addRequest);

        // Get the document by ossKey
        GetDocumentRequest request = new GetDocumentRequest(kbName);
        request.setOssKey(TEST_OSS_KEY);

        GetDocumentResponse response = client.getDocument(request);
        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData());
        assertTrue(response.getData().isEmpty());
    }

    @Test
    public void testGetDocumentResponseFields() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        AddDocumentsRequest addRequest = new AddDocumentsRequest(kbName);

        List<AddDocumentsRequest.DocumentItem> documents = new ArrayList<>();
        documents.add(new AddDocumentsRequest.DocumentItem(TEST_OSS_KEY));
        addRequest.setDocuments(documents);

        client.addDocuments(addRequest);

        GetDocumentRequest request = new GetDocumentRequest(kbName);
        request.setOssKey(TEST_OSS_KEY);

        GetDocumentResponse response = client.getDocument(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testGetDocumentWithNonExistentDocId() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        GetDocumentRequest request = new GetDocumentRequest(kbName);
        request.setDocId("non_existent_doc_id");

        GetDocumentResponse response = client.getDocument(request);
        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData());
        assertTrue(response.getData().isEmpty());
    }

    @Test(expected = Exception.class)
    public void testGetDocumentWithoutDocIdOrOssKey() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        GetDocumentRequest request = new GetDocumentRequest(kbName);

        client.getDocument(request);
    }

    @Test(expected = Exception.class)
    public void testGetDocumentWithoutSubspaceWhenRequired() {
        String kbName = createKnowledgeBaseWithSubspace();
        GetDocumentRequest request = new GetDocumentRequest(kbName);
        request.setOssKey(TEST_OSS_KEY);

        client.getDocument(request);
    }

    @Test(expected = Exception.class)
    public void testGetDocumentFromNonExistentKb() {
        String kbName = "non_existent_kb_" + UUID.randomUUID();
        GetDocumentRequest request = new GetDocumentRequest(kbName);
        request.setOssKey(TEST_OSS_KEY);

        client.getDocument(request);
    }

    // ============================================================================
    // ListDocuments Tests
    // ============================================================================

    @Test
    public void testListDocumentsBasic() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        AddDocumentsRequest addRequest = new AddDocumentsRequest(kbName);

        List<AddDocumentsRequest.DocumentItem> documents = new ArrayList<>();
        documents.add(new AddDocumentsRequest.DocumentItem(TEST_OSS_KEY));
        addRequest.setDocuments(documents);

        client.addDocuments(addRequest);

        ListDocumentsRequest request = new ListDocumentsRequest(kbName);

        ListDocumentsResponse response = client.listDocuments(request);
        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData());
        assertTrue(response.getData().getDocumentDetails().isEmpty());
    }

    @Test
    public void testListDocumentsWithMaxResults() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        AddDocumentsRequest addRequest = new AddDocumentsRequest(kbName);

        List<AddDocumentsRequest.DocumentItem> documents = new ArrayList<>();
        documents.add(new AddDocumentsRequest.DocumentItem(TEST_OSS_KEY));
        addRequest.setDocuments(documents);

        client.addDocuments(addRequest);

        ListDocumentsRequest request = new ListDocumentsRequest(kbName);
        request.setMaxResults(10);

        ListDocumentsResponse response = client.listDocuments(request);
        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData());
        assertTrue(response.getData().getDocumentDetails().isEmpty());
    }

    @Test
    public void testListDocumentsWithPagination() {
        String kbName = createKnowledgeBaseWithoutSubspace();

        // Add multiple documents
        for (int i = 0; i < 5; i++) {
            AddDocumentsRequest addRequest = new AddDocumentsRequest(kbName);
            List<AddDocumentsRequest.DocumentItem> documents = new ArrayList<>();
            documents.add(new AddDocumentsRequest.DocumentItem("oss://bucket/file" + i + ".pdf"));
            addRequest.setDocuments(documents);
            client.addDocuments(addRequest);
        }

        ListDocumentsRequest request = new ListDocumentsRequest(kbName);
        request.setMaxResults(2);

        List<DocumentInfo> allDocs = new ArrayList<>();
        String nextToken = null;

        do {
            if (nextToken != null) {
                request.setNextToken(nextToken);
            }

            ListDocumentsResponse response = client.listDocuments(request);
            assertEquals("SUCCESS", response.getCode());

            if (response.getData().getDocumentDetails() != null) {
                allDocs.addAll(response.getData().getDocumentDetails());
            }

            nextToken = response.getData().getNextToken();
        } while (nextToken != null && !nextToken.isEmpty());

    }

    @Test(expected = Exception.class)
    public void testListDocumentsWithoutSubspaceWhenRequired() {
        String kbName = createKnowledgeBaseWithSubspace();
        ListDocumentsRequest request = new ListDocumentsRequest(kbName);

        client.listDocuments(request);
    }

    @Test(expected = Exception.class)
    public void testListDocumentsFromNonExistentKb() {
        String kbName = "non_existent_kb_" + UUID.randomUUID();
        ListDocumentsRequest request = new ListDocumentsRequest(kbName);

        client.listDocuments(request);
    }

    @Test(expected = Exception.class)
    public void testListDocumentsWithMaxResultsExceedsLimit() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        ListDocumentsRequest request = new ListDocumentsRequest(kbName);
        request.setMaxResults(1001); // Exceeds limit of 1000

        client.listDocuments(request);
    }

    @Test(expected = Exception.class)
    public void testListDocumentsWithNegativeMaxResults() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        ListDocumentsRequest request = new ListDocumentsRequest(kbName);
        request.setMaxResults(-1);

        client.listDocuments(request);
    }

    @Test(expected = Exception.class)
    public void testListDocumentsWithInvalidNextToken() {
        String kbName = createKnowledgeBaseWithoutSubspace();
        ListDocumentsRequest request = new ListDocumentsRequest(kbName);
        request.setNextToken("invalid_next_token");

        client.listDocuments(request);
    }

    @Test
    public void testListDocumentsWithWrongSubspace() {
        String kbName = createKnowledgeBaseWithSubspace();
        ListDocumentsRequest request = new ListDocumentsRequest(kbName);
        request.setSubspace(Collections.singletonList("wrong_subspace"));

        ListDocumentsResponse response = client.listDocuments(request);
        assertEquals("SUCCESS", response.getCode());
        assertNotNull(response.getData());
        assertTrue(response.getData().getDocumentDetails().isEmpty());
    }
}
