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
 * TableStore提供了BatchWriteRow接口让用户能够向TableStore批量导入数据，但是在实际使用过程中，BatchWriteRow接口的使用方式对开发者并不友好。例如用户
 * 需要自己计算并控制一次BatchWriteRow的数据量大小，需要关注行大小或者列大小是否超过TableStore的限制，需要对主键去重以及处理单行错误的重试等等。
 *
 * TableStoreWriter旨在针对批量数据导入的场景提供一个更加易用的接口，但是不是所有场景均适用TableStoreWriter，在使用之前，请详细了解它的行为，否则会造成数据正确性的问题：
 *     - TableStoreWriter所有接口保证线程安全。
 *     - 调用{@link #addRowChange}接口向TableStoreWriter写入一行返回后，并不代表该行已经写入TableStore，该行会保存在TableStoreWriter的本地缓冲中，等待flush，flush成功后该行才会被写入TableStore。
 *     - 向TableStoreWriter写入的行的顺序与最终写入TableStore的行的顺序不保证一致。
 *     - 所有通过TableStoreWriter的写入操作，需要使用者保证是幂等的，因为TableStoreWriter内部默认会启用重试，某一行可能会多次写入。
 *     - TableStoreWriter对缓冲区的flush时机由两个因素来控制：一个是flushInterval，根据时间定期的进行flush；
 *       另一个是{@link com.alicloud.openservices.tablestore.writer.WriterConfig#maxBatchSize}，根据缓冲区的数据量来决定是否需要flush。
 *     - TableStoreWriter在写入数据时，会自动对导入失败的行进行重试，但是并不保证所有行重试后最终一定能够被写入成功（例如：某一行在写入时加了存在性检查，若已经存在，则永远写不成功）。
 *       此时经过若干次重试后若还有数据无法被写入成功，则这些行会被认为是脏数据，脏数据会通过TableStoreCallback反馈给使用者。
 *     - 再向TableStoreWriter写入数据之前，请先注册好Callback，否则若在注册CallBack之前已经有数据写入失败，则失败的行会直接被抛弃。
 *     - 程序退出前，需要显式调用{@link #flush}或{@link #close}，将还存在缓冲区中的数据进行flush，否则会造成这部分数据丢失。
 *
 *  TableStoreWriter对批量导入数据的处理流程：
 *     1. addRowChange接口是线程安全的，支持用户线程并发的写入
 *     2. 通过addRowChange接口写入的数据会暂存到缓冲区
 *     3. 每个TableStoreWriter会启动一个后台导入线程，缓冲区的数据会由该后台线程进行flush。为提升导入效率，该线程会异步并发发送多个RPC，并发数可以配置。
 *     4. 发送成功或者失败的行都会通过callback反馈给使用者，callback的调用会在另外一个ExecutorService（用户可定制）内执行。
 *
 *  用户如何使用TableStoreWriter：
 *     1. 初始化TableStoreWriter，可配置RestrictionConfig、缓冲区Queue、Callback ExecutorService等等
 *     2. 调用{@link #addRowChange}并发的向TableStoreWriter中写入数据
 *     3. 若写入完毕，调用{@link #flush}对缓冲区内的数据进行flush
 *     4. 调用{@link #close}关闭TableStoreWriter，释放资源
 */
public interface TableStoreWriter {
    /**
     * 向本地缓冲区增加一行数据。
     *
     * 在写入缓冲区之前，会对该行数据做如下检查：
     *  - 该行的主键的Schema是否与表定义的相同。
     *  - 该行的主键列或属性列的值大小是否超过限制，值的限制配置在{@link com.alicloud.openservices.tablestore.writer.WriterConfig#maxPKColumnSize}和{@link com.alicloud.openservices.tablestore.writer.WriterConfig#maxAttrColumnSize}。
     *  - 该行的属性列的个数是否超过{@link com.alicloud.openservices.tablestore.writer.WriterConfig#maxColumnsCount}。
     *  - 该行的大小是否超过{@link com.alicloud.openservices.tablestore.writer.WriterConfig#maxBatchSize}。
     *  - 属性列中是否有列名与主键列相同。
     *
     * 若在写入缓冲区之前被判定为脏数据，该部分数据不会触发CallBack的调用。
     *
     * 以上检查会在SDK端带来额外的CPU消耗，但是这些检查是必要的，为了减少不必要的RPC开销，以及避免脏数据污染同一批次Batch写入的行。
     *
     * 注意：若缓冲区满，则该操作会被block。
     *
     * 若判断该行为脏数据，该接口会抛出{@link com.alicloud.openservices.tablestore.ClientException}。
     *
     * @param rowChange 要写入的行
     * @throws com.alicloud.openservices.tablestore.ClientException 若该行被判定为脏数据
     */
    void addRowChange(RowChange rowChange) throws ClientException;

    /**
     * 接口功能同 {@link com.alicloud.openservices.tablestore.TableStoreWriter#addRowChange}一致，
     * 但会返回写入结果的Future，返回该行的写入成功、失败状态
     *
     * @param rowChange 要写入的行
     * @throws com.alicloud.openservices.tablestore.ClientException 若该行被判定为脏数据
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
     * 向本地缓冲区批量写入行。
     *
     * 批量写入的每一行会做与{@link #addRowChange(RowChange)}一样的检查，若批量写入的行中存在脏数据，则该函数会抛{@link ClientException}，
     * 且所有的脏数据会写入dirtyRows。

     * @param rowChanges 批量写入的行
     * @param dirtyRows 用于传出脏数据的列表
     * @throws ClientException 若存在脏数据
     */
    void addRowChange(List<RowChange> rowChanges, List<RowChange> dirtyRows) throws ClientException;


    /**
     * 向本地缓冲区批量写入行。
     *
     * 批量写入的每一行会做与{@link #addRowChange(RowChange)}一样的检查;脏数据会直接更新到WriterResponse中统计

     * @param rowChanges 批量写入的行
     * @throws ClientException 若存在脏数据
     */
    Future<WriterResult> addRowChangeWithFuture(List<RowChange> rowChanges) throws ClientException;

    /**
     * @see  #setResultCallback(TableStoreCallback)
     *
     * @deprecated please change to {@link #setResultCallback(TableStoreCallback)}
     */
    void setCallback(TableStoreCallback<RowChange, ConsumedCapacity> callback);

    /**
     * 设置Callback，数据写入成功或者失败均会通过Callback来反馈。
     *
     * 注意：callback会被写入TableStoreWriter的所有行共享，会被并发的调用。
     * 这与{@link com.alicloud.openservices.tablestore.AsyncClientInterface}中每个接口的Callback不同，其callback会与每个请求一一对应，且可以是独立的。
     * 但是这里的Callback是与RowChange对应，且所有RowChange都共享该callback。
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
     * 获取设置的Callback。
     *
     * @return callback
     */
    TableStoreCallback<RowChange, RowWriteResult> getResultCallback();

    /**
     * 获取限制项配置。
     *
     * @return 限制项配置
     */
    WriterConfig getWriterConfig();

    /**
     * 获取数据导入期间的统计信息。
     *
     * @return 数据导入的统计信息
     */
    WriterStatistics getWriterStatistics();


    /**
     * 主动flush缓冲区中的数据，该函数会等待缓冲区中的所有数据被flush完毕。
     *
     * 注意：若在调用flush之后继续向缓冲区中写入数据，flush不会等待这部分新数据的写入，新数据的flush需要重新调用一次flush。
     */
    void flush() throws ClientException;

    /**
     * 关闭TableStoreWriter，在关闭之前，会先flush掉缓冲区内的所有数据。
     *
     * 注意：若在close过程中或者close之后仍然调用{@link #addRowChange}向缓冲区写入数据，则该部分数据不保证会写入TableStore。
     * addRowChange和close两个操作之间的互斥由调用方来保证，请务必保证在调用close之前，没有任何其他线程会继续使用该writer，否则会有预期外的行为。
     */
    void close();
}
