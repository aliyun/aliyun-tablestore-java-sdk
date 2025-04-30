package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.writer.RowWriteResult;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.writer.WriterResult;
import com.alicloud.openservices.tablestore.writer.WriterStatistics;

import java.util.List;
import java.util.concurrent.Future;

/**
 * TableStore provides the BatchWriteRow interface to allow users to import data in bulk to TableStore. However, in practical usage, the BatchWriteRow interface is not developer-friendly. For example, users need to calculate and control the size of data sent in one BatchWriteRow operation, ensure that the row or column sizes do not exceed TableStore's limits, deduplicate primary keys, and handle retries for single-row errors, among other tasks.
 *
 * TableStoreWriter aims to provide a more user-friendly interface for bulk data import scenarios, but it is not suitable for all use cases. Before using it, please fully understand its behavior; otherwise, data integrity issues may arise:
 *     - All interfaces of TableStoreWriter are thread-safe.
 *     - After calling the {@link #addRowChange} interface to write a row to TableStoreWriter, it does not mean that the row has been written to TableStore yet. The row will be stored in the local buffer of TableStoreWriter, waiting for flush. Only after a successful flush will the row be written to TableStore.
 *     - The order of rows written to TableStoreWriter does not guarantee consistency with the final order of rows written to TableStore.
 *     - All write operations through TableStoreWriter should be idempotent as TableStoreWriter internally enables retries by default, which may result in multiple writes for a single row.
 *     - The timing of flushing the buffer in TableStoreWriter is controlled by two factors: one is the flushInterval, which triggers regular flushes based on time intervals; the other is 
 *       {@link com.alicloud.openservices.tablestore.writer.WriterConfig#maxBatchSize}, which determines whether to flush based on the amount of data in the buffer.
 *     - When writing data, TableStoreWriter automatically retries rows that fail to import, but it does not guarantee that all rows will eventually be successfully written after retries (e.g., if a row includes an existence check and already exists, it will never succeed).
 *       If some data cannot be written successfully after several retries, these rows will be considered dirty data and fed back to the user via TableStoreCallback.
 *     - Before writing data to TableStoreWriter, make sure to register a Callback. Otherwise, if data fails before registering the Callback, the failed rows will be discarded.
 *     - Before exiting the program, explicitly call {@link #flush} or {@link #close} to flush any remaining data in the buffer. Otherwise, this data will be lost.
 *
 *  Data processing flow for bulk imports using TableStoreWriter:
 *     1. The addRowChange interface is thread-safe and supports concurrent writes from user threads.
 *     2. Data written via the addRowChange interface is temporarily stored in the buffer.
 *     3. Each TableStoreWriter starts a background import thread that flushes the buffered data. To improve import efficiency, this thread asynchronously sends multiple RPCs in parallel, with the concurrency level configurable.
 *     4. Both successful and failed rows are fed back to the user via callback, which is executed in another ExecutorService (customizable by the user).
 *
 *  How users can use TableStoreWriter:
 *     1. Initialize TableStoreWriter, configuring options such as RestrictionConfig, buffer Queue, Callback ExecutorService, etc.
 *     2. Call {@link #addRowChange} to concurrently write data into TableStoreWriter.
 *     3. Once finished writing, call {@link #flush} to flush the data in the buffer.
 *     4. Call {@link #close} to close TableStoreWriter and release resources.
 */
public interface TableStoreWriter {
    /**
     * Add a row of data to the local buffer.
     *
     * Before writing to the buffer, the following checks are performed on the row data:
     *  - Whether the schema of the primary key of this row matches the table definition.
     *  - Whether the size of the values in the primary key columns or attribute columns exceeds the limit; the value limits are configured in {@link com.alicloud.openservices.tablestore.writer.WriterConfig#maxPKColumnSize} and {@link com.alicloud.openservices.tablestore.writer.WriterConfig#maxAttrColumnSize}.
     *  - Whether the number of attribute columns in this row exceeds {@link com.alicloud.openservices.tablestore.writer.WriterConfig#maxColumnsCount}.
     *  - Whether the size of this row exceeds {@link com.alicloud.openservices.tablestore.writer.WriterConfig#maxBatchSize}.
     *  - Whether there are any column names in the attribute columns that are the same as those in the primary key columns.
     *
     * If the data is determined to be dirty before being written to the buffer, this portion of the data will not trigger a CallBack invocation.
     *
     * These checks introduce additional CPU consumption on the SDK side, but they are necessary to reduce unnecessary RPC overhead and prevent dirty data from contaminating rows in the same batch write operation.
     *
     * Note: If the buffer is full, this operation will be blocked.
     *
     * If the row is judged to be dirty data, this interface will throw a {@link com.alicloud.openservices.tablestore.ClientException}.
     *
     * @param rowChange The row to be written
     * @throws com.alicloud.openservices.tablestore.ClientException If the row is determined to be dirty data
     */
    void addRowChange(RowChange rowChange) throws ClientException;

    /**
     * The interface function is the same as {@link com.alicloud.openservices.tablestore.TableStoreWriter#addRowChange},
     * but it will return a Future of the write result, indicating the success or failure status of writing this row.
     *
     * @param rowChange The row to be written.
     * @throws com.alicloud.openservices.tablestore.ClientException If the row is determined to be dirty data.
     */
    Future<WriterResult> addRowChangeWithFuture(RowChange rowChange) throws ClientException;

    /**
     * Same with {@link #addRowChange(RowChange)}, but it won't be blocked if the buffer is full.
     *
     * @param rowChange
     * @return true if add succeed, return false if the buffer is full.
     * @throws ClientException
     */
    boolean tryAddRowChange(RowChange rowChange) throws ClientException;

    /**
     * Batch write rows to the local buffer.
     *
     * Each row in the batch write will undergo the same checks as {@link #addRowChange(RowChange)}. 
     * If there is any dirty data in the rows being written in the batch, this function will throw a {@link ClientException}, 
     * and all dirty data will be written to dirtyRows.
     *
     * @param rowChanges Rows for batch writing
     * @param dirtyRows List used to return the dirty data
     * @throws ClientException if there is any dirty data
     */
    void addRowChange(List<RowChange> rowChanges, List<RowChange> dirtyRows) throws ClientException;


    /**
     * Batch write rows to the local buffer.
     *
     * Each row in the batch write will undergo the same checks as {@link #addRowChange(RowChange)}; dirty data will be directly updated in the WriterResponse statistics.
     *
     * @param rowChanges Rows for batch writing
     * @throws ClientException If there is dirty data
     */
    Future<WriterResult> addRowChangeWithFuture(List<RowChange> rowChanges) throws ClientException;

    /**
     * @see  #setResultCallback(TableStoreCallback)
     *
     * @deprecated please change to {@link #setResultCallback(TableStoreCallback)}
     */
    void setCallback(TableStoreCallback<RowChange, ConsumedCapacity> callback);

    /**
     * Set Callback, which will provide feedback when data writing succeeds or fails.
     *
     * Note: The callback will be shared by all rows written by TableStoreWriter and may be called concurrently.
     * This is different from the Callback in each interface of {@link com.alicloud.openservices.tablestore.AsyncClientInterface}, 
     * where the callback corresponds one-to-one with each request and can be independent.
     * However, the Callback here corresponds to RowChange, and all RowChanges share this callback.
     *
     * @param callback
     */
    void setResultCallback(TableStoreCallback<RowChange, RowWriteResult> callback);

    /**
     * @see  #getResultCallback()
     *
     * @deprecated please change to {@link #getResultCallback()}
     */
    TableStoreCallback<RowChange, ConsumedCapacity> getCallback();

    /**
     * Get the set Callback.
     *
     * @return callback
     */
    TableStoreCallback<RowChange, RowWriteResult> getResultCallback();

    /**
     * Get the configuration of the limit item.
     *
     * @return Configuration of the limit item
     */
    WriterConfig getWriterConfig();

    /**
     * Get the statistical information during data import.
     *
     * @return Statistical information of data import
     */
    WriterStatistics getWriterStatistics();


    /**
     * Actively flush the data in the buffer. This function will wait until all data in the buffer has been flushed.
     *
     * Note: If data is written to the buffer after calling flush, the flush will not wait for this new data to be written. The flush of the new data requires a re-call of flush.
     */
    void flush() throws ClientException;

    /**
     * Closes the TableStoreWriter. Before closing, it flushes all data in the buffer.
     *
     * Note: If {@link #addRowChange} is called to write data into the buffer during or after the close process, there is no guarantee that this part of the data will be written to TableStore.
     * The mutual exclusion between addRowChange and close operations must be ensured by the caller. It is essential to ensure that no other threads continue to use this writer before calling close; otherwise, unexpected behavior may occur.
     */
    void close();
}
