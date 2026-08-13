/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 */

package com.alicloud.openservices.tablestore;

import java.util.Iterator;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.delivery.*;
import com.alicloud.openservices.tablestore.model.iterator.RowIterator;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryResponse;
import com.alicloud.openservices.tablestore.model.knowledgebase.*;
import com.alicloud.openservices.tablestore.model.memory.*;

/**
 * The access interface for Alibaba Cloud Table Store (TableStore, formerly OTS).
 * <p>
 * Alibaba Cloud Table Store (TableStore) is a massive data storage and real-time query service built on top of Alibaba Cloud's large-scale distributed computing system.
 * </p>
 */
public interface SyncClientInterface {

    /**
     * Create a new table under the user's instance.
     * <p> The table cannot be read from or written to immediately after creation; you need to wait a few seconds. </p>
     *
     * @param createTableRequest Parameters required for executing CreateTable
     * @return The result returned by the TableStore service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request return result, or network exception encountered
     */
    public CreateTableResponse createTable(CreateTableRequest createTableRequest)
            throws TableStoreException, ClientException;

    /**
     * Dynamically change the configuration or reserved throughput of a table after it has been created.
     * <p>For example, if a user wants to adjust the table's TTL, MaxVersions, etc., or if the user finds that the current reserved throughput is too low and needs to be increased.</p>
     * <p>The UpdateTable operation cannot be used to change the table's TableMeta. The adjustable configurations are:</p>
     * <ul>
     * <li>Reserved throughput ({@link ReservedThroughput}):
     * A table's reserved throughput can be dynamically changed; read or write throughput can be adjusted separately. The minimum time interval for adjusting each table's read/write throughput is 1 minute. If this UpdateTable operation occurs less than 1 minute after the last UpdateTable or CreateTable operation, the request will be rejected.
     * </li>
     * <li>Table options ({@link TableOptions}):
     * Only some configuration items of the table can be dynamically changed, such as TTL, MaxVersions, etc.
     * </li>
     * </ul>
     * After the UpdateTable operation is completed, it will return the table's current reserved throughput and configuration after the changes.
     *
     * @param updateTableRequest Parameters required to execute UpdateTable
     * @return Result returned by the TableStore service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request, or network exception encountered
     */
    public UpdateTableResponse updateTable(UpdateTableRequest updateTableRequest)
        throws TableStoreException, ClientException;

    /**
     * <p>Get the detailed information of a table, which includes:</p>
     * <ul>
     * <li>The structure of the table ({@link TableMeta})</li>
     * <li>The reserved throughput of the table ({@link ReservedThroughputDetails})</li>
     * <li>The configuration parameters of the table ({@link TableOptions})</li>
     * </ul>
     *
     * @param describeTableRequest Parameters required to execute DescribeTable
     * @return The result returned by the TableStore service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request or network exception encountered
     */
    public DescribeTableResponse describeTable(DescribeTableRequest describeTableRequest)
        throws TableStoreException, ClientException;

    /**
     * Returns the list of all tables under the user's current instance.
     *
     * @return The result returned by the TableStore service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request response or network exception encountered
     */
    public ListTableResponse listTable() throws TableStoreException, ClientException;

    /**
     * Delete a table under a specified instance for the user.
     * <p>Caution: After the table is successfully deleted, all data under the table will be cleared and cannot be recovered. Please operate with caution!</p>
     *
     * @param deleteTableRequest Parameters required to execute DeleteTable
     * @return The result returned by the TableStore service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid return result of the request or network exception encountered
     */
    public DeleteTableResponse deleteTable(DeleteTableRequest deleteTableRequest)
        throws TableStoreException, ClientException;

    /**
     * Create a global table using an existing table as the base table.
     * @param createGlobalTableRequest Parameters required to execute CreateGlobalTable
     * @return Results returned by the Table Store service
     * @throws TableStoreException Exceptions returned by the Table Store service
     * @throws ClientException The requested result is invalid, or a network exception occurs.
     */
    public CreateGlobalTableResponse createGlobalTable(CreateGlobalTableRequest createGlobalTableRequest) throws TableStoreException, ClientException;
    /**
     * Adds a new physical table copy to the global table at the specified location.
     * @param bindGlobalTableRequest Parameters required to execute BindGlobalTable
     * @return Results returned by the Table Store service
     * @throws TableStoreException Exceptions returned by the Table Store service
     * @throws ClientException The requested result is invalid, or a network exception occurs.
     */
    public BindGlobalTableResponse bindGlobalTable(BindGlobalTableRequest bindGlobalTableRequest) throws TableStoreException, ClientException;
    /**
     * Unbind the specified physical table from the global table. The unbinding operation will not delete the physical table.
     * <p>Note: After the table is successfully unbound, it will no longer synchronize data with the global table, nor can it be bound to the global table again. Please operate with caution!</p>
     * @param unbindGlobalTableRequest Parameters required to execute UnbindGlobalTable
     * @return Results returned by the Table Store service
     * @throws TableStoreException Exceptions returned by the Table Store service
     * @throws ClientException The requested result is invalid, or a network exception occurs.
     */
    public UnbindGlobalTableResponse unbindGlobalTable(UnbindGlobalTableRequest unbindGlobalTableRequest) throws TableStoreException, ClientException;
    /**
     * Get the physical table information and status of the global table.
     * @param descGlobalTableRequest Parameters required to execute DescribeGlobalTable
     * @return Results returned by the Table Store service
     * @throws TableStoreException Exceptions returned by the Table Store service
     * @throws ClientException The requested result is invalid, or a network exception occurs.
     */
    public DescribeGlobalTableResponse describeGlobalTable(DescribeGlobalTableRequest descGlobalTableRequest) throws TableStoreException, ClientException;
    /**
     * Modify the read and write permissions of the physical table in the global table.
     * <p>Note: This will cause write operations on disabled physical tables to fail!</p>
     *
     * @param updateGlobalTableRequest Parameters required to execute UpdateGlobalTable
     * @return Results returned by the Table Store service
     * @throws TableStoreException Exceptions returned by the Table Store service
     * @throws ClientException The requested result is invalid, or a network exception occurs.
     */
    public UpdateGlobalTableResponse updateGlobalTable(UpdateGlobalTableRequest updateGlobalTableRequest) throws TableStoreException, ClientException;

    /**
     * Create an index table under a specified table as indicated by the user.
     *
     * @param createIndexRequest
     * @return The result returned by the TableStore service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException The returned result is invalid, or a network exception was encountered
     */
    public CreateIndexResponse createIndex(CreateIndexRequest createIndexRequest)
        throws TableStoreException, ClientException;

    /**
     * Delete a specific index table under a specified table.
     * <p>Caution: After the index table is successfully deleted, all data under that index table will be cleared and cannot be recovered. Please operate with caution!</p>
     *
     * @param deleteIndexRequest Parameters required to execute DeleteIndex
     * @return The result returned by the TableStore service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid return result of the request or network exception encountered
     */
    public DeleteIndexResponse deleteIndex(DeleteIndexRequest deleteIndexRequest)
        throws TableStoreException, ClientException;

    /**
     * Add a predefined column to a table.
     * @param addDefinedColumnRequest Parameters required for executing AddDefinedColumn.
     * @return The result returned by the TableStore service.
     * @throws TableStoreException An exception returned by the TableStore service.
     * @throws ClientException The returned result of the request is invalid, or a network exception occurred.
     */
    public AddDefinedColumnResponse addDefinedColumn(AddDefinedColumnRequest addDefinedColumnRequest)
        throws TableStoreException, ClientException;

    /**
     * Delete a predefined column for a table
     * @param deleteDefinedColumnRequest Parameters required to execute DeleteDefinedColumn
     * @return The result returned by the TableStore service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request return or network exception encountered
     */
    public DeleteDefinedColumnResponse deleteDefinedColumn(DeleteDefinedColumnRequest deleteDefinedColumnRequest)
        throws TableStoreException, ClientException;

    /**
     * Read a single row of data from the table.
     *
     * @param getRowRequest Parameters required to perform the GetRow operation.
     * @return The result returned by the TableStore service.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid request response or network exception encountered.
     */
    public GetRowResponse getRow(GetRowRequest getRowRequest)
            throws TableStoreException, ClientException;

    /**
     * Insert or overwrite a row of data in the table.
     * <p>If the row to be written already exists, the old row will be deleted and a new one will be written.</p>
     * <p>If the row to be written does not exist, a new row will be directly written.</p>
     *
     * @param putRowRequest Parameters required for performing the PutRow operation.
     * @return The result returned by the TableStore service.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid response from the request or network exception encountered.
     */
    public PutRowResponse putRow(PutRowRequest putRowRequest)
            throws TableStoreException, ClientException;

    /**
     * Updates a row of data in the table.
     * <p>If the row to be updated does not exist, a new row of data is written.</p>
     * <p>The update operation can include writing a new attribute column or deleting one or more versions of an attribute column.</p>
     *
     * @param updateRowRequest Parameters required for the UpdateRow operation.
     * @return The result returned by the TableStore service.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException The returned result of the request is invalid, or a network exception was encountered.
     */
    public UpdateRowResponse updateRow(UpdateRowRequest updateRowRequest)
            throws TableStoreException, ClientException;

    /**
     * Delete a row of data from the table.
     * <p>If the row exists, it will be deleted.</p>
     * <p>If the row does not exist, this operation will have no effect.</p>
     *
     * @param deleteRowRequest Parameters required for performing the DeleteRow operation.
     * @return The result returned by the TableStore service.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid response or network exception encountered during the request.
     */
    public DeleteRowResponse deleteRow(DeleteRowRequest deleteRowRequest)
            throws TableStoreException, ClientException;

    /**
     * Read multiple rows of data from multiple tables.
     * <p>The BatchGetRow operation can be regarded as a collection of multiple GetRow operations. Each operation is executed independently, returns results independently, and calculates service capability units independently.</p>
     * <p>Compared to executing a large number of GetRow operations, using the BatchGetRow operation can effectively reduce the response time of requests and increase the data read rate.</p>
     * <p>However, note that BatchGetRow only supports setting query conditions at the table level. After the operation is completed, the status of each sub-request needs to be checked individually, and retries can be attempted for failed rows.</p>
     *
     * @param batchGetRowRequest Parameters required to execute the BatchGetRow operation.
     * @return The result returned by the TableStore service.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid request return result or network exception encountered.
     */
    public BatchGetRowResponse batchGetRow(BatchGetRowRequest batchGetRowRequest)
            throws TableStoreException, ClientException;

    /**
     * Perform update or delete operations on multiple rows across multiple tables.
     * <p>The BatchWriteRow operation can be considered as a collection of multiple PutRow, UpdateRow, and DeleteRow operations. Each operation is executed independently, returns results independently, and calculates service capability units independently.</p>
     * <p>After executing the BatchWriteRow operation, it is necessary to check the status of each sub-request individually to determine the write result and choose to retry for failed rows.</p>
     *
     * @param batchWriteRowRequest Parameters required to execute the BatchWriteRow operation.
     * @return The result returned by the TableStore service.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid response or network exception encountered during the request.
     */
    public BatchWriteRowResponse batchWriteRow(BatchWriteRowRequest batchWriteRowRequest)
    		throws TableStoreException, ClientException;

    /**
     * Perform update or delete operations on multiple rows in a single table, offline service interface.
     * <p>The BulkImport operation can be regarded as a collection of multiple PutRow, UpdateRow, DeleteRow operations. Each operation is executed independently, returns results independently, and consumes units independently.</p>
     * <p>After executing the BulkImport operation, it is necessary to check the status of each sub-request individually to determine the write result and choose to retry the failed rows.</p>
     *
     * @param bulkImportRequest Parameters required to execute the BatchWriteRow operation.
     * @return The result returned by the TableStore service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request return result, or network exception encountered
     */
    public BulkImportResponse bulkImport(BulkImportRequest bulkImportRequest)
            throws TableStoreException, ClientException;

    /**
     * Query multiple rows of data within a range from the table.
     *
     * @param getRangeRequest Parameters required for performing the GetRange operation.
     * @return The result returned by the TableStore service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request return or network exception encountered
     */
    public GetRangeResponse getRange(GetRangeRequest getRangeRequest)
            throws TableStoreException, ClientException;

    /**
     * Query multiple rows of data within a range from the table, offline service interface.
     *
     * @param bulkExportRequest Parameters required to execute the GetRange operation.
     * @return The result returned by the TableStore service.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid request return or network exception encountered.
     */
    public BulkExportResponse bulkExport(BulkExportRequest bulkExportRequest)
            throws TableStoreException, ClientException;

    /**
     * Splits the data of a table into chunks based on a certain data size, and returns the split information for use by the data retrieval interface. The returned data chunks are arranged in ascending order of the primary key column. Each chunk of data contains the hash value of the partition ID where the chunk is located, as well as the primary key values of the starting and ending rows, following the left-closed-right-open interval.
     *
     * @param computeSplitsBySizeRequest Parameters required to execute the ComputeSplitsBySize operation.
     * @return Result returned by the TableStore service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request response or network exception encountered
     */
    public ComputeSplitsBySizeResponse computeSplitsBySize(ComputeSplitsBySizeRequest computeSplitsBySizeRequest)
            throws TableStoreException, ClientException;

    /**
     * Encapsulates the {@link #getRange(GetRangeRequest)} interface in iterator form.
     *
     * @param rangeIteratorParameter The parameters required to execute the createRangeIterator operation.
     * @return Iterator for rows
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request response or network exception encountered
     */
    public Iterator<Row> createRangeIterator(
            RangeIteratorParameter rangeIteratorParameter) throws TableStoreException,
            ClientException;

    public Iterator<Row> createBulkExportIterator(
            RangeIteratorParameter rangeIteratorParameter) throws TableStoreException,
            ClientException;

    public WideColumnIterator createWideColumnIterator(GetRowRequest getRowRequest) throws TableStoreException,
            ClientException;

    /**
     * Get the entire Stream list under the user's current instance or the Stream of a specific table.
     *
     * @param listStreamRequest Parameters required for performing the ListStream operation.
     * @return The result returned by the TableStore service.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid request response or network exception encountered.
     */
    public ListStreamResponse listStream(ListStreamRequest listStreamRequest)
            throws TableStoreException, ClientException;

    /**
     * Get the detailed information of the specified Stream. Use this method to obtain the Shard list.
     *
     * @param describeStreamRequest Parameters required for the DescribeStream operation.
     * @return The result returned by the TableStore service.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid request return or network exception encountered.
     */
    public DescribeStreamResponse describeStream(DescribeStreamRequest describeStreamRequest)
            throws TableStoreException, ClientException;

    /**
     * Get the ShardIterator, which can be used to read data from a Shard.
     *
     * @param getShardIteratorRequest Parameters required to execute the GetShardIterator operation.
     * @return The result returned by the TableStore service.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid request response or network exception encountered.
     */
    public GetShardIteratorResponse getShardIterator(GetShardIteratorRequest getShardIteratorRequest)
            throws TableStoreException, ClientException;

    /**
     * Read data from a Shard using a ShardIterator.
     *
     * @param getStreamRecordRequest Parameters required to perform the GetStreamRecord operation.
     * @return The result returned by the TableStore service.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid request response or network exception encountered.
     */
    public GetStreamRecordResponse getStreamRecord(GetStreamRecordRequest getStreamRecordRequest)
            throws TableStoreException, ClientException;

    /**
     * Create SearchIndex
     * @param request Parameters required for creating a SearchIndex, see {@link CreateSearchIndexRequest}
     * @return The creation result returned by the SearchIndex service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request or network exception encountered
     */
    public CreateSearchIndexResponse createSearchIndex(CreateSearchIndexRequest request)
            throws TableStoreException, ClientException;

    /**
     * Update SearchIndex (for user index exchange, or setting index query weight)
     * @param request Parameters required for updating the SearchIndex, see {@link UpdateSearchIndexRequest}
     * @return The result returned by the SearchIndex service after creation
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request response or network exception encountered
     */
    public UpdateSearchIndexResponse updateSearchIndex(UpdateSearchIndexRequest request)
            throws TableStoreException, ClientException;

    /**
     * Get the SearchIndex list under a table.
     * <p>There can be multiple SearchIndex tables under a single table. Through this function, you can retrieve all SearchIndex information under a table.</p>
     * @param request Parameters required to get the SearchIndex list.
     * @return The SearchIndex list under the specified TableStore table.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid response result or network exception encountered during the request.
     */
    public ListSearchIndexResponse listSearchIndex(ListSearchIndexRequest request)
            throws TableStoreException, ClientException;

    /**
     * Delete SearchIndex
     * <p>Specify the tableName and indexName to delete an index</p>
     * <p>Note: It is not allowed to delete a table before all indexes under the table are deleted</p>
     * @param request Parameters required for deleting SearchIndex
     * @return The deletion result returned after the deletion of the SearchIndex service is executed
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request return or network exception encountered
     */
    public DeleteSearchIndexResponse deleteSearchIndex(DeleteSearchIndexRequest request)
            throws TableStoreException, ClientException;

    /**
     * Get the information of a SearchIndex.
     * @param request The parameters required to get the SearchIndex (tableName and indexName).
     * @return Returns the schema of the specified index and its current synchronization status information.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid response from the request or a network exception encountered.
     */
    public DescribeSearchIndexResponse describeSearchIndex(DescribeSearchIndexRequest request)
            throws TableStoreException, ClientException;

    /**
     * Get the partition information of the data.
     *
     * @param request Parameters required to perform the computeSplits operation.
     * @return Results returned by the Tablestore service
     * @throws TableStoreException   Exception returned by Tablestore service.
     * @throws ClientException The return result of the request is invalid or a network exception was encountered.
     */
    public ComputeSplitsResponse computeSplits(ComputeSplitsRequest request)
        throws TableStoreException, ClientException;

    /**
     * Scan data form SearchIndex.
     *
     * @param request Parameters required to perform the parallelScan operation.
     * @return Results returned by the Tablestore service
     * @throws TableStoreException   Exception returned by Tablestore service.
     * @throws ClientException The return result of the request is invalid or a network exception was encountered.
     */
    public ParallelScanResponse parallelScan(ParallelScanRequest request)
        throws TableStoreException, ClientException;

    /**
     * Get the Iterator for ParallelScan.
     * @param request Parameters required to perform ParallelScan operation.
     * @return Iterator for ParallelScan.
     * @throws TableStoreException   Exception returned by Tablestore service.
     * @throws ClientException The return result of the request is invalid or a network exception was encountered.
     *
     */
    public RowIterator createParallelScanIterator(ParallelScanRequest request) throws TableStoreException, ClientException;


    /**
     * Get the Iterator for Search.
     * <p>Note: If your searchIndex has a nested field and you want to search data, please specify the Sort. </p>
     * @param request Parameters required to perform searchRequest operation.
     * @return Iterator for SearchQuery.
     * @throws TableStoreException   Exception returned by Tablestore service.
     * @throws ClientException The return result of the request is invalid or a network exception was encountered.
     *
     */
    public RowIterator createSearchIterator(SearchRequest request) throws TableStoreException, ClientException;

    /**
     * Search functionality
     * <p>Build your own SearchRequest, then obtain the SearchResponse</p>
     * <p>Example:</p>
     * <p>
     *     <code>
     *               SearchQuery searchQuery = new SearchQuery();
     *               TermQuery termQuery = new TermQuery();
     *               termQuery.setFieldName("user_name");
     *               termQuery.setTerm("jay");
     *               searchQuery.setQuery(termQuery);
     *               SearchRequest searchRequest = new SearchRequest(tableName, indexName, searchQuery);
     *               SearchResponse resp = ots.search(searchRequest);
     *      </code>
     * </p>
     * @param request Parameters required for performing a search, see {@link SearchRequest} for details
     * @return Search results, see {@link SearchResponse} for details
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request or network exception encountered
     */
    public SearchResponse search(SearchRequest request)
            throws TableStoreException, ClientException;

    /**
     * Start a local transaction
     * @param request Parameters required for initiating a local transaction operation
     * @return
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException The response is invalid or a network exception occurs
     */
    public StartLocalTransactionResponse startLocalTransaction(StartLocalTransactionRequest request)
            throws TableStoreException, ClientException;

    /**
     * Commit a transaction
     * @param request Parameters required for the commit transaction operation
     * @return
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request response or network exception encountered
     */
    public CommitTransactionResponse commitTransaction(CommitTransactionRequest request)
            throws TableStoreException, ClientException;

    /**
     * Cancel a transaction
     * @param request Parameters required for the cancel transaction operation
     * @return
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request response or network exception encountered
     */
    public AbortTransactionResponse abortTransaction(AbortTransactionRequest request)
            throws TableStoreException, ClientException;

    /**
     * Convert to the asynchronous interface Client.
     * @return  Asynchronous interface Client.
     */
    public AsyncClientInterface asAsyncClient();

    /**
     * Release resources.
     * <p>Make sure to release resources after all requests have been executed. After releasing resources, no further requests can be sent, and ongoing requests may not return results.</p>
     */
    public void shutdown();

    /**
     * Switch CredentialsProvider.
     *
     * @param newCrdsProvider new CredentialsProvider, see {@link com.alicloud.openservices.tablestore.core.auth.CredentialsProviderFactory}.
     */
    public void switchCredentialsProvider(CredentialsProvider newCrdsProvider);

    /**
     * Create a data delivery task
     * @param request Parameters required to create the delivery task
     * @return
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request or network exception encountered
     */
    public CreateDeliveryTaskResponse createDeliveryTask(CreateDeliveryTaskRequest request)
            throws TableStoreException, ClientException;

    /**
     * Delete the delivery task
     * @param request Parameters required for deleting the delivery task
     * @return
     * @throws TableStoreException Exception returned by the Tablestore service
     * @throws ClientException The request results are invalid, or a network exception is encountered
     */
    public DeleteDeliveryTaskResponse deleteDeliveryTask(DeleteDeliveryTaskRequest request)
            throws TableStoreException, ClientException;

    /**
     * Delivery task description function
     * @param request Parameters required to describe the delivery task
     * @return
     * @throws TableStoreException Exception returned by the Tablestore service
     * @throws ClientException The request result is invalid or a network exception is encountered
     */
    public DescribeDeliveryTaskResponse describeDeliveryTask(DescribeDeliveryTaskRequest request)
            throws TableStoreException, ClientException;

    /**
     * List all delivery tasks under the user table
     * @param request Parameters required to get the list of delivery tasks
     * @return
     * @throws TableStoreException Exception returned by the Tablestore service
     * @throws ClientException The result of the request is invalid, or a network exception was encountered
     */
    public ListDeliveryTaskResponse listDeliveryTask(ListDeliveryTaskRequest request)
            throws TableStoreException, ClientException;

    /**
     * SQL query request
     * @param request Parameters for SQL query
     * @return
     * @throws TableStoreException Exception returned by the Tablestore service
     * @throws ClientException Request result is invalid or a network exception is encountered
     */
    public SQLQueryResponse sqlQuery(SQLQueryRequest request) throws TableStoreException, ClientException;

    // KnowledgeBase operations
    public CreateKnowledgeBaseResponse createKnowledgeBase(CreateKnowledgeBaseRequest request) throws TableStoreException, ClientException;

    public DescribeKnowledgeBaseResponse describeKnowledgeBase(DescribeKnowledgeBaseRequest request) throws TableStoreException, ClientException;

    public ListKnowledgeBaseResponse listKnowledgeBase(ListKnowledgeBaseRequest request) throws TableStoreException, ClientException;

    public DeleteKnowledgeBaseResponse deleteKnowledgeBase(DeleteKnowledgeBaseRequest request) throws TableStoreException, ClientException;

    public UpdateKnowledgeBaseResponse updateKnowledgeBase(UpdateKnowledgeBaseRequest request) throws TableStoreException, ClientException;

    // Document operations
    public AddDocumentsResponse addDocuments(AddDocumentsRequest request) throws TableStoreException, ClientException;

    public GetDocumentResponse getDocument(GetDocumentRequest request) throws TableStoreException, ClientException;

    public ListDocumentsResponse listDocuments(ListDocumentsRequest request) throws TableStoreException, ClientException;

    public DeleteDocumentsResponse deleteDocuments(DeleteDocumentsRequest request) throws TableStoreException, ClientException;

    public UpdateDocumentResponse updateDocument(UpdateDocumentRequest request) throws TableStoreException, ClientException;

    // Chunk operations
    public ListChunksResponse listChunks(ListChunksRequest request) throws TableStoreException, ClientException;

    public UpdateChunksResponse updateChunks(UpdateChunksRequest request) throws TableStoreException, ClientException;

    // Retrieval operations
    public RetrieveResponse retrieve(RetrieveRequest request) throws TableStoreException, ClientException;

    // Memory Store operations
    /**
     * Creates a memory store.
     *
     * @param request parameters for the CreateMemoryStore operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default CreateMemoryStoreResponse createMemoryStore(CreateMemoryStoreRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("createMemoryStore is not implemented by this client");
    }

    /**
     * Gets a memory store.
     *
     * @param request parameters for the GetMemoryStore operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default GetMemoryStoreResponse getMemoryStore(GetMemoryStoreRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("getMemoryStore is not implemented by this client");
    }

    /**
     * Lists memory stores.
     *
     * @param request parameters for the ListMemoryStores operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default ListMemoryStoresResponse listMemoryStores(ListMemoryStoresRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("listMemoryStores is not implemented by this client");
    }

    /**
     * Updates a memory store.
     *
     * @param request parameters for the UpdateMemoryStore operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default UpdateMemoryStoreResponse updateMemoryStore(UpdateMemoryStoreRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("updateMemoryStore is not implemented by this client");
    }

    /**
     * Deletes a memory store.
     *
     * @param request parameters for the DeleteMemoryStore operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default DeleteMemoryStoreResponse deleteMemoryStore(DeleteMemoryStoreRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("deleteMemoryStore is not implemented by this client");
    }

    /**
     * Adds messages to a memory store for extraction.
     *
     * @param request parameters for the AddMemories operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default AddMemoriesResponse addMemories(AddMemoriesRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("addMemories is not implemented by this client");
    }

    /**
     * Searches memories in a memory store.
     *
     * @param request parameters for the SearchMemories operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default SearchMemoriesResponse searchMemories(SearchMemoriesRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("searchMemories is not implemented by this client");
    }

    /**
     * Lists memories in a memory store.
     *
     * @param request parameters for the ListMemories operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default ListMemoriesResponse listMemories(ListMemoriesRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("listMemories is not implemented by this client");
    }

    /**
     * Gets a memory by ID.
     *
     * @param request parameters for the GetMemory operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default GetMemoryResponse getMemory(GetMemoryRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("getMemory is not implemented by this client");
    }

    /**
     * Updates a memory.
     *
     * @param request parameters for the UpdateMemory operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default UpdateMemoryResponse updateMemory(UpdateMemoryRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("updateMemory is not implemented by this client");
    }

    /**
     * Deletes a memory.
     *
     * @param request parameters for the DeleteMemory operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default DeleteMemoryResponse deleteMemory(DeleteMemoryRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("deleteMemory is not implemented by this client");
    }

    /**
     * Lists session messages stored for a scope.
     *
     * @param request parameters for the ListMemoryStoreMessages operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default ListMemoryStoreMessagesResponse listMemoryStoreMessages(ListMemoryStoreMessagesRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("listMemoryStoreMessages is not implemented by this client");
    }

    /**
     * Lists memory service request records.
     *
     * @param request parameters for the ListMemoryStoreRequests operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default ListMemoryStoreRequestsResponse listMemoryStoreRequests(ListMemoryStoreRequestsRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("listMemoryStoreRequests is not implemented by this client");
    }

    /**
     * Gets the task associated with a memory request.
     *
     * @param request parameters for the GetMemoryTask operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default GetMemoryTaskResponse getMemoryTask(GetMemoryTaskRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("getMemoryTask is not implemented by this client");
    }

    /**
     * Lists memory tasks.
     *
     * @param request parameters for the ListMemoryTasks operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default ListMemoryTasksResponse listMemoryTasks(ListMemoryTasksRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("listMemoryTasks is not implemented by this client");
    }

    /**
     * Lists scopes that contain memories in a memory store.
     *
     * @param request parameters for the ListMemoryStoreScopes operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default ListMemoryStoreScopesResponse listMemoryStoreScopes(ListMemoryStoreScopesRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("listMemoryStoreScopes is not implemented by this client");
    }

    /**
     * Creates a memory dream task.
     *
     * @param request parameters for the CreateMemoryDreamTask operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default CreateMemoryDreamTaskResponse createMemoryDreamTask(CreateMemoryDreamTaskRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("createMemoryDreamTask is not implemented by this client");
    }

    /**
     * Gets a memory dream task.
     *
     * @param request parameters for the GetMemoryDreamTask operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default GetMemoryDreamTaskResponse getMemoryDreamTask(GetMemoryDreamTaskRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("getMemoryDreamTask is not implemented by this client");
    }

    /**
     * Lists memory dream tasks.
     *
     * @param request parameters for the ListMemoryDreamTasks operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default ListMemoryDreamTasksResponse listMemoryDreamTasks(ListMemoryDreamTasksRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("listMemoryDreamTasks is not implemented by this client");
    }

    /**
     * Cancels a memory dream task.
     *
     * @param request parameters for the CancelMemoryDreamTask operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default CancelMemoryDreamTaskResponse cancelMemoryDreamTask(CancelMemoryDreamTaskRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("cancelMemoryDreamTask is not implemented by this client");
    }

    /**
     * Lists actions produced by memory dream tasks.
     *
     * @param request parameters for the ListMemoryDreamActions operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default ListMemoryDreamActionsResponse listMemoryDreamActions(ListMemoryDreamActionsRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("listMemoryDreamActions is not implemented by this client");
    }

    /**
     * Applies actions produced by memory dream tasks.
     *
     * @param request parameters for the ApplyMemoryDreamActions operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default ApplyMemoryDreamActionsResponse applyMemoryDreamActions(ApplyMemoryDreamActionsRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("applyMemoryDreamActions is not implemented by this client");
    }

    // File Memory Item operations
    /**
     * Adds a file memory item.
     *
     * @param request parameters for the AddItem operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default AddItemResponse addItem(AddItemRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("addItem is not implemented by this client");
    }

    /**
     * Lists file memory items.
     *
     * @param request parameters for the ListItems operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default ListItemsResponse listItems(ListItemsRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("listItems is not implemented by this client");
    }

    /**
     * Gets a file memory item.
     *
     * @param request parameters for the GetItem operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default GetItemResponse getItem(GetItemRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("getItem is not implemented by this client");
    }

    /**
     * Updates a file memory item.
     *
     * @param request parameters for the UpdateItem operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default UpdateItemResponse updateItem(UpdateItemRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("updateItem is not implemented by this client");
    }

    /**
     * Deletes a file memory item.
     *
     * @param request parameters for the DeleteItem operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default DeleteItemResponse deleteItem(DeleteItemRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("deleteItem is not implemented by this client");
    }

    /**
     * Lists versions of a file memory item.
     *
     * @param request parameters for the ListItemVersions operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default ListItemVersionsResponse listItemVersions(ListItemVersionsRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("listItemVersions is not implemented by this client");
    }

    /**
     * Gets a version of a file memory item.
     *
     * @param request parameters for the GetItemVersion operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default GetItemVersionResponse getItemVersion(GetItemVersionRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("getItemVersion is not implemented by this client");
    }

    /**
     * Redacts a version of a file memory item.
     *
     * @param request parameters for the RedactItemVersion operation
     * @return the result returned by the TableStore service
     * @throws TableStoreException exception returned by the TableStore service
     * @throws ClientException invalid response or network exception
     * @throws UnsupportedOperationException if the client does not implement this operation
     */
    default RedactItemVersionResponse redactItemVersion(RedactItemVersionRequest request) throws TableStoreException, ClientException {
        throw new UnsupportedOperationException("redactItemVersion is not implemented by this client");
    }

}
