package com.alicloud.openservices.tablestore;

import java.util.concurrent.Future;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.delivery.*;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryResponse;

public interface AsyncClientInterface {

    /**
     * Create a new table under the user's instance.
     * <p>After the table is created, it cannot be read from or written to immediately; you need to wait for a few seconds.</p>
     *
     * @param createTableRequest Parameters required to execute the CreateTable operation
     * @param callback The callback function to invoke after the request is completed; can be null, which means no callback function needs to be executed
     * @return A Future for obtaining the result
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request, or network exceptions encountered
     */
    public Future<CreateTableResponse> createTable(
        CreateTableRequest createTableRequest,
        TableStoreCallback<CreateTableRequest, CreateTableResponse> callback);

    /**
     * Dynamically change the configuration or reserved throughput of a table after it has been created.
     * <p>For example, a user may want to adjust the table's TTL, MaxVersions configurations, or if the user finds that the current reserved throughput is too low and needs to increase the reserved throughput.</p>
     * <p>The UpdateTable operation cannot be used to modify the table's TableMeta. The adjustable configurations are:</p>
     * <ul>
     * <li>Reserved throughput ({@link ReservedThroughput}):
     * The reserved throughput of the table can be dynamically changed, and both read and write throughputs can be modified separately. The minimum time interval for adjusting the read/write throughput of each table is 1 minute.
     * If the current UpdateTable operation occurs less than 1 minute after the previous UpdateTable or CreateTable operation, the request will be rejected.
     * </li>
     * <li>Table options ({@link TableOptions}):
     * Only some configuration items of the table can be dynamically changed, such as TTL, MaxVersions, etc.
     * </li>
     * </ul>
     * After the UpdateTable operation is completed, it will return the reserved throughput and configuration of the table after the current changes.
     *
     * @param updateTableRequest Parameters required to execute UpdateTable
     * @param callback The callback function to be invoked upon completion of the request, which can be null if no callback function is needed
     * @return A Future for obtaining the result
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request or network exceptions encountered
     */
    public Future<UpdateTableResponse> updateTable(
        UpdateTableRequest updateTableRequest,
        TableStoreCallback<UpdateTableRequest, UpdateTableResponse> callback);

    /**
     * <p>Get the detailed information of a table, which includes:</p>
     * <ul>
     * <li>The structure of the table ({@link TableMeta})</li>
     * <li>The reserved throughput of the table ({@link ReservedThroughputDetails})</li>
     * <li>The configuration parameters of the table ({@link TableOptions})</li>
     * </ul>
     *
     * @param describeTableRequest Parameters required to execute DescribeTable
     * @param callback The callback function to be invoked upon request completion, can be null if no callback is needed
     * @return A Future for obtaining the result
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request or network exception encountered
     */
    public Future<DescribeTableResponse> describeTable(
        DescribeTableRequest describeTableRequest,
        TableStoreCallback<DescribeTableRequest, DescribeTableResponse> callback);

    /**
     * Returns the list of all tables under the user's current instance.
     *
     * @param callback The callback function to be called after the request is completed, can be null, which means no callback function is needed
     * @return The Future for obtaining the result
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request or network exception encountered
     */
    public Future<ListTableResponse> listTable(
        TableStoreCallback<ListTableRequest, ListTableResponse> callback);

    /**
     * Deletes a table under a specified instance for the user.
     * <p>Caution: After the table is successfully deleted, all data under the table will be cleared and cannot be recovered. Please operate with caution!</p>
     *
     * @param deleteTableRequest Parameters required to execute DeleteTable
     * @param callback The callback function to be invoked after the request is completed. It can be null, which means no callback function is needed.
     * @return Future for obtaining the result
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request or network exception encountered
     */
    public Future<DeleteTableResponse> deleteTable(
        DeleteTableRequest deleteTableRequest,
        TableStoreCallback<DeleteTableRequest, DeleteTableResponse> callback);

    /**
     * Create an index table under a specified table as indicated by the user.
     *
     * @param createIndexRequest Parameters required to execute CreateIndex
     * @param callback The callback function to be invoked upon completion of the request, can be null which means no callback function is needed
     * @return Future for obtaining the result
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException The result returned from the request is invalid, or a network exception was encountered
     */
    public Future<CreateIndexResponse> createIndex(
        CreateIndexRequest createIndexRequest,
        TableStoreCallback<CreateIndexRequest, CreateIndexResponse> callback);

    /**
     * Deletes an index table under a specific table as designated by the user.
     *
     * @param deleteIndexRequest Parameters required to execute the deleteIndex operation.
     * @param callback The callback function to be invoked upon completion of the request, can be null if no callback is needed.
     * @return A Future for obtaining the result.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Request resulted in invalid response or encountered network issues.
     */
    public Future<DeleteIndexResponse> deleteIndex(
        DeleteIndexRequest deleteIndexRequest,
        TableStoreCallback<DeleteIndexRequest, DeleteIndexResponse> callback);

    /**
     * Add predefined columns to the table specified by the user
     * @param addDefinedColumnRequest Parameters required for executing addDefinedColumn
     * @param callback The callback function to be called upon request completion, can be null which means no callback function is needed
     * @return Future for obtaining the result
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException The result of the request is invalid, or a network exception was encountered
     */
    public Future<AddDefinedColumnResponse> addDefinedColumn(
            AddDefinedColumnRequest addDefinedColumnRequest,
            TableStoreCallback<AddDefinedColumnRequest, AddDefinedColumnResponse> callback);

    /**
     * Deletes a predefined column for the specified table.
     * @param deleteDefinedColumnRequest Parameters required to execute deleteDefinedColumn.
     * @param callback The callback function to be called upon request completion; can be null, which means no callback function is needed.
     * @return Future for obtaining the result.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid results returned from the request, or network exceptions encountered.
     */
    public Future<DeleteDefinedColumnResponse> deleteDefinedColumn(
        DeleteDefinedColumnRequest deleteDefinedColumnRequest,
        TableStoreCallback<DeleteDefinedColumnRequest, DeleteDefinedColumnResponse> callback);

    /**
     * Read a single row of data from the table.
     *
     * @param getRowRequest The parameters required to perform the GetRow operation.
     * @param callback The callback function to be invoked upon request completion, can be null which means no callback function is needed.
     * @return A Future for obtaining the result.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid response from the request or network exception encountered.
     */
    public Future<GetRowResponse> getRow(
            GetRowRequest getRowRequest, TableStoreCallback<GetRowRequest, GetRowResponse> callback);

    /**
     * Insert or overwrite a row of data in the table.
     * <p>If the row to be written already exists, the old row will be deleted and the new row will be written.</p>
     * <p>If the row to be written does not exist, the new row will be written directly.</p>
     *
     * @param putRowRequest Parameters required for the PutRow operation.
     * @param callback The callback function to be invoked after the request is completed. It can be null, indicating that no callback function needs to be executed.
     * @return Future for obtaining the result
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid return result of the request or network exception encountered
     */
    public Future<PutRowResponse> putRow(
            PutRowRequest putRowRequest, TableStoreCallback<PutRowRequest, PutRowResponse> callback);

    /**
     * Update a row of data in the table.
     * <p>If the row to be updated does not exist, a new row of data will be written.</p>
     * <p>The update operation can include writing a new attribute column or deleting one or more versions of an attribute column.</p>
     *
     * @param updateRowRequest Parameters required for performing the UpdateRow operation.
     * @param callback The callback function to be invoked upon request completion, can be null which means no callback is needed.
     * @return A Future for obtaining the result.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid response from the request or network exceptions encountered.
     */
    public Future<UpdateRowResponse> updateRow(
            UpdateRowRequest updateRowRequest, TableStoreCallback<UpdateRowRequest, UpdateRowResponse> callback);

    /**
     * Delete a row of data from the table.
     * <p>If the row exists, it will be deleted.</p>
     * <p>If the row does not exist, this operation will have no effect.</p>
     *
     * @param deleteRowRequest Parameters required for performing the DeleteRow operation.
     * @param callback The callback function to invoke upon request completion, can be null which means no callback is needed.
     * @return A Future for obtaining the result.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid response or network exception encountered during the request.
     */
    public Future<DeleteRowResponse> deleteRow(
            DeleteRowRequest deleteRowRequest, TableStoreCallback<DeleteRowRequest, DeleteRowResponse> callback);

    /**
     * Read multiple rows of data from multiple tables.
     * <p>The BatchGetRow operation can be considered as a collection of multiple GetRow operations. Each operation is executed independently, returns results independently, and calculates the service capability unit independently.</p>
     * <p>Compared to executing a large number of GetRow operations, using the BatchGetRow operation can effectively reduce the response time of requests and improve the data read rate.</p>
     * <p>However, it should be noted that BatchGetRow only supports setting query conditions at the table level. After the operation is completed, you need to check the status of each sub-request individually and choose to retry for failed rows.</p>
     *
     * @param batchGetRowRequest Parameters required to perform the BatchGetRow operation.
     * @param callback The callback function to invoke after the request is completed, can be null, which means no callback function needs to be executed
     * @return The Future for obtaining the result
     */
    public Future<BatchGetRowResponse> batchGetRow(
            BatchGetRowRequest batchGetRowRequest, TableStoreCallback<BatchGetRowRequest, BatchGetRowResponse> callback);

    /**
     * Perform update or delete operations on multiple rows in multiple tables.
     * <p>The BatchWriteRow operation can be considered as a collection of multiple PutRow, UpdateRow, and DeleteRow operations. Each operation is executed independently, returns results independently, and calculates the capacity unit independently.</p>
     * <p>After executing the BatchWriteRow operation, you need to check the status of each sub-request one by one to determine the write result and choose to retry for failed rows.</p>
     *
     * @param batchWriteRowRequest Parameters required for executing the BatchWriteRow operation.
     * @param callback The callback function to be invoked upon request completion, can be null if no callback function is needed.
     * @return A Future for obtaining the result.
     */
    public Future<BatchWriteRowResponse> batchWriteRow(
            BatchWriteRowRequest batchWriteRowRequest, TableStoreCallback<BatchWriteRowRequest, BatchWriteRowResponse> callback);

    /**
     * Perform update or delete operations on multiple rows within a single table; this is an offline service API.
     * <p>The BulkImport operation can be regarded as a collection of multiple PutRow, UpdateRow, and DeleteRow operations. Each operation is executed independently, returns results independently, and calculates consumed capacity units independently.</p>
     * <p>After executing the BulkImport operation, you need to check the status of each sub-request individually to determine the write result and choose whether to retry for failed rows.</p>
     *
     * @param bulkImportRequest Parameters required to execute the BatchWriteRow operation.
     * @param callback The callback function to invoke upon request completion, which can be null if no callback function needs to be executed.
     * @return A Future to obtain the result.
     */
    public Future<BulkImportResponse> bulkImport(
            BulkImportRequest bulkImportRequest, TableStoreCallback<BulkImportRequest, BulkImportResponse> callback);

    /**
     * Query multiple rows of data within a range from the table.
     *
     * @param getRangeRequest The parameters required to perform the GetRange operation.
     * @param callback The callback function to be invoked upon request completion, can be null if no callback function is needed.
     * @return Future for obtaining the results.
     */
    public Future<GetRangeResponse> getRange(
            GetRangeRequest getRangeRequest, TableStoreCallback<GetRangeRequest, GetRangeResponse> callback);

    /**
     * Query multiple rows of data within a range from the table, offline service interface.
     *
     * @param bulkExportRequest Parameters required for performing the GetRange operation.
     * @param callback The callback function to be called upon request completion, can be null which means no callback function is needed.
     * @return Future for obtaining the result.
     */
    public Future<BulkExportResponse> bulkExport(
            BulkExportRequest bulkExportRequest, TableStoreCallback<BulkExportRequest, BulkExportResponse> callback);

    /**
     * Splits the data of the table into chunks based on a certain data size, and returns the split information for use by the data retrieval interface. The returned data chunks are arranged in ascending order of the primary key column. Each chunk of data information contains the hash value of the partition ID where the chunk is located, as well as the primary key values of the start and end rows, following the left-closed-right-open interval.
     *
     * @param computeSplitsBySizeRequest Parameters required to execute the ComputeSplitsBySize operation.
     * @param callback The callback function to be called upon completion of the request, can be null which means no callback function needs to be executed.
     * @return The Future for obtaining the result.
     */
    public Future<ComputeSplitsBySizeResponse> computeSplitsBySize(ComputeSplitsBySizeRequest computeSplitsBySizeRequest, TableStoreCallback<ComputeSplitsBySizeRequest, ComputeSplitsBySizeResponse> callback);

    /**
     * Get the entire Stream list under the user's current instance or the Stream of a specific table.
     *
     * @param listStreamRequest Parameters required for executing the ListStream operation.
     * @param callback The callback function to be called upon completion of the request, can be null which means no callback function is needed
     * @return Future for obtaining the result
     */
    public Future<ListStreamResponse> listStream(
            ListStreamRequest listStreamRequest, TableStoreCallback<ListStreamRequest, ListStreamResponse> callback);

    /**
     * Get the detailed information of the specified Stream. Use this method to obtain the Shard list.
     *
     * @param describeStreamRequest The parameters required for the DescribeStream operation.
     * @param callback The callback function to be called upon completion of the request, can be null, which means no callback function is needed.
     * @return The Future for obtaining the result.
     */
    public Future<DescribeStreamResponse> describeStream(
            DescribeStreamRequest describeStreamRequest, TableStoreCallback<DescribeStreamRequest, DescribeStreamResponse> callback);

    /**
     * Get the ShardIterator, which can be used to read data from a Shard.
     *
     * @param getShardIteratorRequest Parameters required for the GetShardIterator operation.
     * @param callback The callback function to be invoked upon completion of the request, can be null if no callback is needed.
     * @return A Future for obtaining the result.
     */
    public Future<GetShardIteratorResponse> getShardIterator(
            GetShardIteratorRequest getShardIteratorRequest, TableStoreCallback<GetShardIteratorRequest, GetShardIteratorResponse> callback);

    /**
     * Read data from a Shard using a ShardIterator.
     *
     * @param getStreamRecordRequest The parameters required to execute the GetStreamRecord operation.
     * @param callback The callback function to be invoked upon completion of the request, can be null if no callback is needed.
     * @return A Future for obtaining the result.
     */
    public Future<GetStreamRecordResponse> getStreamRecord(
            GetStreamRecordRequest getStreamRecordRequest, TableStoreCallback<GetStreamRecordRequest, GetStreamRecordResponse> callback);

    /**
     * Start a local transaction
     * @param request Parameters required for initiating a local transaction operation
     * @return
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request response or network exception encountered
     */
    public Future<StartLocalTransactionResponse> startLocalTransaction(
            StartLocalTransactionRequest request, TableStoreCallback<StartLocalTransactionRequest, StartLocalTransactionResponse> callback);

    /**
     * Commit a transaction
     * @param request Parameters required for the commit transaction operation
     * @return
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request response or network exception encountered
     */
    public Future<CommitTransactionResponse> commitTransaction(
            CommitTransactionRequest request, TableStoreCallback<CommitTransactionRequest, CommitTransactionResponse> callback);

    /**
     * Cancel a transaction
     * @param request Parameters required for the cancel transaction operation
     * @return
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request response or network exception encountered
     */
    public Future<AbortTransactionResponse> abortTransaction(
            AbortTransactionRequest request, TableStoreCallback<AbortTransactionRequest, AbortTransactionResponse> callback);

    /**
     * Create SearchIndex
     * @param request Parameters required for creating a SearchIndex, see {@link CreateSearchIndexRequest}
     * @param callback The callback function to be called upon completion of the request, can be null which means no callback function is needed
     * @return The creation result returned by the SearchIndex service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request or network exception encountered
     */
    public Future<CreateSearchIndexResponse> createSearchIndex(
            CreateSearchIndexRequest request, TableStoreCallback<CreateSearchIndexRequest, CreateSearchIndexResponse> callback);

    /**
     * Update SearchIndex (used for swapping indexes or setting index query weights). It is recommended to prioritize the use of the official website console to achieve the dynamic modification of the multi-field index schema.
     * @param request   Parameters required to update the SearchIndex
     * @param callback  The callback function to be called upon completion of the request, can be null which means no callback function needs to be executed
     * @return The creation result returned by the SearchIndex service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request or network exception encountered
     */
    public Future<UpdateSearchIndexResponse> updateSearchIndex(
            UpdateSearchIndexRequest request, TableStoreCallback<UpdateSearchIndexRequest, UpdateSearchIndexResponse> callback);

    /**
     * Get the SearchIndex list under a table.
     * <p>There can be multiple SearchIndex tables under a single table. Through this function, you can retrieve all SearchIndex information for a specific table.</p>
     * @param request Parameters required to get the SearchIndex list.
     * @param callback The callback function to call upon completion of the request, can be null if no callback is needed.
     * @return The SearchIndex list under the specified TableStore table.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid response from the request or network exception encountered.
     */
    public Future<ListSearchIndexResponse> listSearchIndex(
            ListSearchIndexRequest request, TableStoreCallback<ListSearchIndexRequest, ListSearchIndexResponse> callback);

    /**
     * Delete SearchIndex
     * <p>Specify the tableName and indexName to delete an index.</p>
     * <p>Note: It is not allowed to delete a table until all indexes under the table have been deleted.</p>
     * @param request Parameters required for deleting SearchIndex
     * @param callback The callback function to be called upon completion of the request, can be null which means no callback function needs to be executed
     * @return The deletion result returned after executing the delete SearchIndex service
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request or network exception encountered
     */
    public Future<DeleteSearchIndexResponse> deleteSearchIndex(
            DeleteSearchIndexRequest request, TableStoreCallback<DeleteSearchIndexRequest, DeleteSearchIndexResponse> callback);

    /**
     * Get the information of a SearchIndex.
     * @param request Parameters required to get the SearchIndex (tableName and indexName).
     * @param callback The callback function to be called upon completion of the request, can be null which means no callback function is needed.
     * @return Returns the schema of the specified index and its current synchronization status information.
     * @throws TableStoreException Exception returned by the TableStore service.
     * @throws ClientException Invalid response from the request or network exception encountered.
     */
    public Future<DescribeSearchIndexResponse> describeSearchIndex(
            DescribeSearchIndexRequest request, TableStoreCallback<DescribeSearchIndexRequest, DescribeSearchIndexResponse> callback);

    /**
     * Get the partition information of the data
     *
     * @param request Parameters required to perform the computeSplits operation.
     * @param callback The callback function invoked after the request is completed. It can be null, which means that no callback function is required.
     * @throws TableStoreException   Exception returned by Tablestore service.
     * @throws ClientException The return result of the request is invalid or a network exception was encountered.
     */
    public Future<ComputeSplitsResponse> computeSplits(ComputeSplitsRequest request, TableStoreCallback<ComputeSplitsRequest, ComputeSplitsResponse> callback);

    /**
     * Scan data form SearchIndex.
     *
     * @param request Parameters required to perform the parallelScan operation.
     * @param callback The callback function invoked after the request is completed. It can be null, which means that no callback function is required.
     * @throws TableStoreException   Exception returned by Tablestore service.
     * @throws ClientException The return result of the request is invalid or a network exception was encountered.
     */
    public Future<ParallelScanResponse> parallelScan(ParallelScanRequest request, TableStoreCallback<ParallelScanRequest, ParallelScanResponse> callback);

    /**
     * Create a delivery task
     * @param request Parameters required to create a delivery task
     * @param callback The callback function to be called upon completion of the request, can be null which means no callback function is needed
     * @return
     */
    public Future<CreateDeliveryTaskResponse> createDeliveryTask(
            CreateDeliveryTaskRequest request, TableStoreCallback<CreateDeliveryTaskRequest, CreateDeliveryTaskResponse> callback);

    /**
     * Delete the delivery task
     * @param request The parameters required to delete the delivery task
     * @return The deletion result returned after the deletion of the delivery task is executed
     * @throws TableStoreException Exception returned by the Tablestore service
     * @throws ClientException Invalid return result of the request, or network exception encountered
     */
    public Future<DeleteDeliveryTaskResponse> deleteDeliveryTask(
            DeleteDeliveryTaskRequest request, TableStoreCallback<DeleteDeliveryTaskRequest, DeleteDeliveryTaskResponse> callback);

    /**
     * Describe the delivery task
     * @param request Parameters required to describe the delivery task
     * @param callback The callback function to be called upon completion of the request, can be null which means no callback function is needed
     * @return
     */
    public Future<DescribeDeliveryTaskResponse> describeDeliveryTask(
            DescribeDeliveryTaskRequest request, TableStoreCallback<DescribeDeliveryTaskRequest, DescribeDeliveryTaskResponse> callback);

    /**
     * List the delivery task list
     * @param request Parameters required to list the delivery task list
     * @param callback The callback function to be called upon completion of the request, can be null which means no callback function is needed
     * @return
     */
    public Future<ListDeliveryTaskResponse> listDeliveryTask(
            ListDeliveryTaskRequest request, TableStoreCallback<ListDeliveryTaskRequest, ListDeliveryTaskResponse> callback);

    /**
     * Search functionality
     * <p>Build your own SearchRequest, then get the SearchResponse</p>
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
     * @param request  Parameters required for performing the search, see {@link SearchRequest} for more details
     * @param callback The callback function to be called upon completion of the request, can be null which means no callback function is needed
     * @return  Search results, see {@link SearchResponse} for more details
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request or network exception encountered
     */
    public Future<SearchResponse> search(
            SearchRequest request, TableStoreCallback<SearchRequest, SearchResponse> callback);


    /**
     * Convert to the Client of synchronous interface.
     * @return  Synchronous Client
     */
    public SyncClientInterface asSyncClient();

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

    public Future<SQLQueryResponse> sqlQuery(SQLQueryRequest request, TableStoreCallback<SQLQueryRequest, SQLQueryResponse> callback);
}
