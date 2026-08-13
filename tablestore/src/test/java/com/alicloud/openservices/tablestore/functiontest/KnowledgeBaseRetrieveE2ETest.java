package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.model.knowledgebase.*;
import com.alicloud.openservices.tablestore.model.knowledgebase.DenseVectorSearchConfiguration;
import com.alicloud.openservices.tablestore.model.knowledgebase.DenseVectorSearchConfiguration;
import com.alicloud.openservices.tablestore.model.knowledgebase.FullTextSearchConfiguration;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/**
 * E2E tests for Knowledge Base Retrieve API. Based on Python SDK test coverage from agent-storage-e2e-test/tests/api_tests
 */
public class KnowledgeBaseRetrieveE2ETest {

    private SyncClient client;
    private List<String> createdKnowledgeBases = new ArrayList<>();
    private static final String KB_NAME_PREFIX = "test_kb_retrieve_";

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

    private String createKnowledgeBaseWithMetadata(boolean subspace, List<MetadataField> metadata) {
        String kbName = generateKbName();
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest(kbName);
        request.setSubspace(subspace);
        if (metadata != null && !metadata.isEmpty()) {
            request.setMetadata(metadata);
        }

        CreateKnowledgeBaseResponse response = OTSHelper.createKnowledgeBaseWithRetry(client, request);
        assertEquals("SUCCESS", response.getCode());
        createdKnowledgeBases.add(kbName);
        return kbName;
    }

    private List<MetadataField> createTestMetadataFields() {
        List<MetadataField> metadata = new ArrayList<>();
        metadata.add(new MetadataField("category", "string"));
        metadata.add(new MetadataField("author", "string"));
        metadata.add(new MetadataField("year", "long"));
        metadata.add(new MetadataField("priority", "long"));
        metadata.add(new MetadataField("tags", "string_list"));
        return metadata;
    }

    // ============================================================================
    // Retrieve Normal Scenario Tests
    // ============================================================================

    @Test
    public void testRetrieveWithDenseVector() {
        String kbName = createKnowledgeBaseWithMetadata(false, null);

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.DENSE_VECTOR));
        DenseVectorSearchConfiguration denseVectorConfig = new DenseVectorSearchConfiguration();
        denseVectorConfig.setNumberOfResults(10);
        config.setDenseVectorSearchConfiguration(denseVectorConfig);
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testRetrieveWithFulltextSearch() {
        String kbName = createKnowledgeBaseWithMetadata(false, null);

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        FullTextSearchConfiguration fulltextConfig = new FullTextSearchConfiguration();
        fulltextConfig.setNumberOfResults(10);
        config.setFullTextSearchConfiguration(fulltextConfig);
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testRetrieveWithHybridSearchRrf() {
        String kbName = createKnowledgeBaseWithMetadata(false, null);

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Arrays.asList(SearchType.DENSE_VECTOR, SearchType.FULL_TEXT));

        RerankingConfiguration rerankingConfig = new RerankingConfiguration();
        rerankingConfig.setType(RerankingType.RRF);

        RRFConfiguration rrfConfig = new RRFConfiguration();
        rrfConfig.setDenseVectorSearchWeight(1.0);
        rrfConfig.setFullTextSearchWeight(1.0);
        rrfConfig.setK(60);
        rerankingConfig.setRRFConfiguration(rrfConfig);

        config.setRerankingConfiguration(rerankingConfig);
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testRetrieveWithMultipleSubspaces() {
        String kbName = createKnowledgeBaseWithMetadata(true, null);

        RetrieveRequest request = new RetrieveRequest(kbName);
        request.setSubspace(Arrays.asList("subspace1", "subspace2"));

        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        FullTextSearchConfiguration fulltextConfig = new FullTextSearchConfiguration();
        fulltextConfig.setNumberOfResults(10);
        config.setFullTextSearchConfiguration(fulltextConfig);
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    // ============================================================================
    // Retrieve Filter Tests
    // ============================================================================

    @Test
    public void testFilterEquals() {
        String kbName = createKnowledgeBaseWithMetadata(false, createTestMetadataFields());

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        config.setFilter(MetadataFilter.equals("category", "cloud"));
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testFilterNotEquals() {
        String kbName = createKnowledgeBaseWithMetadata(false, createTestMetadataFields());

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        config.setFilter(MetadataFilter.notEquals("author", "test_author"));
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testFilterGreaterThan() {
        String kbName = createKnowledgeBaseWithMetadata(false, createTestMetadataFields());

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        config.setFilter(MetadataFilter.greaterThan("year", 2023));
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testFilterLessThan() {
        String kbName = createKnowledgeBaseWithMetadata(false, createTestMetadataFields());

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        config.setFilter(MetadataFilter.lessThan("priority", 5));
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testFilterIn() {
        String kbName = createKnowledgeBaseWithMetadata(false, createTestMetadataFields());

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        config.setFilter(MetadataFilter.in("category", "cloud", "programming"));
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testFilterNotIn() {
        String kbName = createKnowledgeBaseWithMetadata(false, createTestMetadataFields());

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        config.setFilter(MetadataFilter.notIn("author", "test_author"));
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testFilterListContains() {
        String kbName = createKnowledgeBaseWithMetadata(false, createTestMetadataFields());

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        config.setFilter(MetadataFilter.listContains("tags", "cloud"));
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testFilterAndAll() {
        String kbName = createKnowledgeBaseWithMetadata(false, createTestMetadataFields());

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        config.setFilter(MetadataFilter.andAll(MetadataFilter.equals("category", "cloud"), MetadataFilter.greaterThanOrEquals("year", 2024)));
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testFilterOrAll() {
        String kbName = createKnowledgeBaseWithMetadata(false, createTestMetadataFields());

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        config.setFilter(MetadataFilter.orAll(MetadataFilter.equals("author", "test_author"), MetadataFilter.equals("author", "python_expert")));
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testFilterNestedAndOr() {
        String kbName = createKnowledgeBaseWithMetadata(false, createTestMetadataFields());

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        // Create nested filter: (category=cloud AND year>=2024) OR author=test_author
        MetadataFilter andFilter = MetadataFilter.andAll(MetadataFilter.equals("category", "cloud"), MetadataFilter.greaterThanOrEquals("year", 2024));
        MetadataFilter orFilter = MetadataFilter.orAll(andFilter, MetadataFilter.equals("author", "test_author"));

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        config.setFilter(orFilter);
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testFilterWithFulltextSearch() {
        String kbName = createKnowledgeBaseWithMetadata(false, createTestMetadataFields());

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        FullTextSearchConfiguration fulltextConfig = new FullTextSearchConfiguration();
        fulltextConfig.setNumberOfResults(10);
        config.setFullTextSearchConfiguration(fulltextConfig);
        config.setFilter(MetadataFilter.equals("category", "cloud"));
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testFilterWithHybridSearch() {
        String kbName = createKnowledgeBaseWithMetadata(false, createTestMetadataFields());

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Arrays.asList(SearchType.DENSE_VECTOR, SearchType.FULL_TEXT));

        RerankingConfiguration rerankingConfig = new RerankingConfiguration();
        rerankingConfig.setType(RerankingType.RRF);

        RRFConfiguration rrfConfig = new RRFConfiguration();
        rrfConfig.setDenseVectorSearchWeight(1.0);
        rrfConfig.setFullTextSearchWeight(1.0);
        rrfConfig.setK(60);
        rerankingConfig.setRRFConfiguration(rrfConfig);

        config.setRerankingConfiguration(rerankingConfig);
        config.setFilter(MetadataFilter.equals("category", "cloud"));
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    // ============================================================================
    // Retrieve Boundary Scenario Tests
    // ============================================================================

    @Test
    public void testRetrieveWithMinQueryLength() {
        String kbName = createKnowledgeBaseWithMetadata(false, null);

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("a");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        FullTextSearchConfiguration fulltextConfig = new FullTextSearchConfiguration();
        fulltextConfig.setNumberOfResults(10);
        config.setFullTextSearchConfiguration(fulltextConfig);
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testRetrieveWithMaxQueryLength() {
        String kbName = createKnowledgeBaseWithMetadata(false, null);

        // Create 128 character query
        StringBuilder queryText = new StringBuilder();
        for (int i = 0; i < 128; i++) {
            queryText.append("a");
        }

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery(queryText.toString());
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        FullTextSearchConfiguration fulltextConfig = new FullTextSearchConfiguration();
        fulltextConfig.setNumberOfResults(10);
        config.setFullTextSearchConfiguration(fulltextConfig);
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testRetrieveWithMaxNumberOfResults() {
        String kbName = createKnowledgeBaseWithMetadata(false, null);

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        FullTextSearchConfiguration fulltextConfig = new FullTextSearchConfiguration();
        fulltextConfig.setNumberOfResults(100);
        config.setFullTextSearchConfiguration(fulltextConfig);
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testRetrieveWithMinNumberOfResults() {
        String kbName = createKnowledgeBaseWithMetadata(false, null);

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        FullTextSearchConfiguration fulltextConfig = new FullTextSearchConfiguration();
        fulltextConfig.setNumberOfResults(0);
        config.setFullTextSearchConfiguration(fulltextConfig);
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    @Test
    public void testRetrieveWithRrfKOne() {
        String kbName = createKnowledgeBaseWithMetadata(false, null);

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Arrays.asList(SearchType.DENSE_VECTOR, SearchType.FULL_TEXT));

        RerankingConfiguration rerankingConfig = new RerankingConfiguration();
        rerankingConfig.setType(RerankingType.RRF);

        RRFConfiguration rrfConfig = new RRFConfiguration();
        rrfConfig.setDenseVectorSearchWeight(1.0);
        rrfConfig.setFullTextSearchWeight(1.0);
        rrfConfig.setK(1);
        rerankingConfig.setRRFConfiguration(rrfConfig);

        config.setRerankingConfiguration(rerankingConfig);
        request.setRetrievalConfiguration(config);

        RetrieveResponse response = client.retrieve(request);
        assertEquals("SUCCESS", response.getCode());
    }

    // ============================================================================
    // Retrieve Exception Scenario Tests
    // ============================================================================

    @Test(expected = Exception.class)
    public void testRetrieveWithoutKbName() {
        RetrieveRequest request = new RetrieveRequest();
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        client.retrieve(request);
    }

    @Test(expected = Exception.class)
    public void testRetrieveWithEmptyQueryText() {
        String kbName = createKnowledgeBaseWithMetadata(false, null);

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        request.setRetrievalConfiguration(config);

        client.retrieve(request);
    }

    @Test(expected = Exception.class)
    public void testRetrieveWithQueryExceedsMaxLength() {
        String kbName = createKnowledgeBaseWithMetadata(false, null);

        // Create 129 character query (exceeds 128 limit)
        StringBuilder queryText = new StringBuilder();
        for (int i = 0; i < 129; i++) {
            queryText.append("a");
        }

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery(queryText.toString());
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        request.setRetrievalConfiguration(config);

        client.retrieve(request);
    }

    @Test(expected = Exception.class)
    public void testRetrieveWithoutSubspaceWhenRequired() {
        String kbName = createKnowledgeBaseWithMetadata(true, null);

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        request.setRetrievalConfiguration(config);

        client.retrieve(request);
    }

    @Test(expected = Exception.class)
    public void testRetrieveFromNonExistentKb() {
        String nonExistentKb = "non_existent_kb_" + UUID.randomUUID().toString();

        RetrieveRequest request = new RetrieveRequest(nonExistentKb);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Collections.singletonList(SearchType.FULL_TEXT));
        request.setRetrievalConfiguration(config);

        client.retrieve(request);
    }

    @Test(expected = Exception.class)
    public void testRetrieveWithRrfKZero() {
        String kbName = createKnowledgeBaseWithMetadata(false, null);

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Arrays.asList(SearchType.DENSE_VECTOR, SearchType.FULL_TEXT));

        RerankingConfiguration rerankingConfig = new RerankingConfiguration();
        rerankingConfig.setType(RerankingType.RRF);

        RRFConfiguration rrfConfig = new RRFConfiguration();
        rrfConfig.setDenseVectorSearchWeight(1.0);
        rrfConfig.setFullTextSearchWeight(1.0);
        rrfConfig.setK(0);
        rerankingConfig.setRRFConfiguration(rrfConfig);

        config.setRerankingConfiguration(rerankingConfig);
        request.setRetrievalConfiguration(config);

        client.retrieve(request);
    }

    @Test(expected = Exception.class)
    public void testRetrieveWithNegativeRrfK() {
        String kbName = createKnowledgeBaseWithMetadata(false, null);

        RetrieveRequest request = new RetrieveRequest(kbName);
        RetrieveRequest.RetrievalQuery query = new RetrieveRequest.RetrievalQuery("test query");
        query.setType("TEXT");
        request.setRetrievalQuery(query);

        RetrievalConfiguration config = new RetrievalConfiguration();
        config.setSearchType(Arrays.asList(SearchType.DENSE_VECTOR, SearchType.FULL_TEXT));

        RerankingConfiguration rerankingConfig = new RerankingConfiguration();
        rerankingConfig.setType(RerankingType.RRF);

        RRFConfiguration rrfConfig = new RRFConfiguration();
        rrfConfig.setDenseVectorSearchWeight(1.0);
        rrfConfig.setFullTextSearchWeight(1.0);
        rrfConfig.setK(-1);
        rerankingConfig.setRRFConfiguration(rrfConfig);

        config.setRerankingConfiguration(rerankingConfig);
        request.setRetrievalConfiguration(config);

        client.retrieve(request);
    }

}
