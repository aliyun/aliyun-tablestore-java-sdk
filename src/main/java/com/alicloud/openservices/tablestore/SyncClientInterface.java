/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.alicloud.openservices.tablestore;

import java.util.Iterator;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.*;

/**
 * 阿里云表格存储（TableStore, 原OTS）的访问接口。
 * <p>
 * 阿里云表格存储（TableStore）是构建在阿里云大规模分布式计算系统之上的海量数据存储与实时查询的服务。
 * </p>
 */
public interface SyncClientInterface {

    /**
     * 在用户的实例下创建一张新的表。
     * <p>表被创建后不能立即进行读写操作, 需要等待几秒钟. </p>
     *
     * @param createTableRequest 执行CreateTable所需的参数
     * @return TableStore服务返回的结果
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public CreateTableResponse createTable(CreateTableRequest createTableRequest)
            throws TableStoreException, ClientException;

    /**
     * 在表被创建之后，动态的更改表的配置或预留吞吐量。
     * <p>例如用户想要调整表的TTL、MaxVersions等配置或者用户发现当前预留吞吐量过小需要上调预留吞吐量。</p>
     * <p>UpdateTable操作不能用于更改表的TableMeta，可以调整的配置为:</p>
     * <ul>
     * <li>预留吞吐量({@link ReservedThroughput}):
     * 表的预留吞吐量可被动态更改,读或写吞吐量都可以分别单独更改。调整每个表读写吞吐量的最小时间间隔为 1 分钟，
     * 如果本次 UpdateTable 操作距上次 UpdateTable 或者 CreateTable 操作不到 1 分钟的话该请求将被拒绝。
     * </li>
     * <li>表的配置({@link TableOptions}):
     * 只有表的部分配置项可以允许被动态更改，例如TTL、MaxVersions等。
     * </li>
     * </ul>
     * UpdateTable操作执行完毕后，会返回表的当前更改之后的预留吞吐量以及配置。
     *
     * @param updateTableRequest 执行UpdateTable所需的参数
     * @return TableStore服务返回的结果
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public UpdateTableResponse updateTable(UpdateTableRequest updateTableRequest)
        throws TableStoreException, ClientException;

    /**
     * <p>获取表的详细信息，表的详细信息包括：</p>
     * <ul>
     * <li>表的结构({@link TableMeta})</li>
     * <li>表的预留吞吐量({@link ReservedThroughputDetails})</li>
     * <li>表的配置参数({@link TableOptions})</li>
     * </ul>
     *
     * @param describeTableRequest 执行DescribeTable所需的参数
     * @return TableStore服务返回的结果
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public DescribeTableResponse describeTable(DescribeTableRequest describeTableRequest)
        throws TableStoreException, ClientException;

    /**
     * 返回用户当前实例下的所有表的列表。
     *
     * @return TableStore服务返回的结果
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public ListTableResponse listTable() throws TableStoreException, ClientException;

    /**
     * 删除用户指定的某个实例下的一张表。。
     * <p>注意：表被成功删除后该表下所有的数据都将会被清空,无法恢复,请谨慎操作!</p>
     *
     * @param deleteTableRequest 执行DeleteTable所需的参数
     * @return TableStore服务返回的结果
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public DeleteTableResponse deleteTable(DeleteTableRequest deleteTableRequest)
        throws TableStoreException, ClientException;

    /**
     * 在用户指定的某张表下创建一张索引表
     *
     * @param createIndexRequest
     * @return TableStore服务返回的结果
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求返回的结果无效，或遇到网络异常
     */
    public CreateIndexResponse createIndex(CreateIndexRequest createIndexRequest)
        throws TableStoreException, ClientException;

    /**
     * 删除用户指定的某张表下的某张索引表
     * <p>注意：索引表被成功删除后该索引表下所有的数据都将被清空，无法恢复，请谨慎操作！</p>
     *
     * @param deleteIndexRequest 招待DeleteIndex所需的参数
     * @return TableStore服务返回的结果
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public DeleteIndexResponse deleteIndex(DeleteIndexRequest deleteIndexRequest)
        throws TableStoreException, ClientException;

    /**
     * 读取表中的一行数据。
     *
     * @param getRowRequest 执行GetRow操作所需的参数。
     * @return TableStore服务返回的结果
     * @throws TableStoreException    TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public GetRowResponse getRow(GetRowRequest getRowRequest)
            throws TableStoreException, ClientException;

    /**
     * 向表中插入或覆盖一行数据。
     * <p>若要写入的行已经存在，则旧行会被删除后写入新的一行。</p>
     * <p>若要写入的行不存在，则直接写入新的一行。</p>
     *
     * @param putRowRequest 执行PutRow操作所需的参数。
     * @return TableStore服务返回的结果
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public PutRowResponse putRow(PutRowRequest putRowRequest)
            throws TableStoreException, ClientException;

    /**
     * 更新表中的一行数据。
     * <p>若要更新的行不存在，则新写入一行数据。</p>
     * <p>更新操作可以包括新写入一个属性列或者删除一个属性列的一个或多个版本。</p>
     *
     * @param updateRowRequest 执行UpdateRow操作所需的参数。
     * @return TableStore服务返回的结果
     * @throws TableStoreException    TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public UpdateRowResponse updateRow(UpdateRowRequest updateRowRequest)
            throws TableStoreException, ClientException;

    /**
     * 删除表中的一行数据。
     * <p>若该行存在，则删除该行。</p>
     * <p>若该行不存在，则该操作不产生任何影响。</p>
     *
     * @param deleteRowRequest 执行DeleteRow操作所需的参数。
     * @return TableStore服务返回的结果
     * @throws TableStoreException    TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public DeleteRowResponse deleteRow(DeleteRowRequest deleteRowRequest)
            throws TableStoreException, ClientException;

    /**
     * 从多张表中读取多行数据。
     * <p>BatchGetRow 操作可视为多个 GetRow 操作的集合，各个操作独立执行，独立返回结果，独立计算服务能力单元。</p>
     * <p>与执行大量的 GetRow 操作相比,使用 BatchGetRow 操作可以有效减少请求的响应时间，提高数据的读取速率。</p>
     * <p>但需要注意的是 BatchGetRow 只支持在表级别设置查询条件。操作完成后，需要逐个检查子请求的状态，并选择对失败的行进行重试。</p>
     *
     * @param batchGetRowRequest 执行BatchGetRow操作所需的参数。
     * @return TableStore服务返回的结果
     * @throws TableStoreException    TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public BatchGetRowResponse batchGetRow(BatchGetRowRequest batchGetRowRequest)
            throws TableStoreException, ClientException;

    /**
     * 对多张表中对多行执行更新或者删除操作。
     * <p>BatchWriteRow 操作可视为多个PutRow、UpdateRow、DeleteRow 操作的集合，各个操作独立执行，独立返回结果，独立计算服务能力单元。</p>
     * <p>执行 BatchWriteRow 操作后，需要逐个检查子请求的状态，来判断写入结果，并选择对失败的行进行重试。</p>
     *
     * @param batchWriteRowRequest 执行BatchWriteRow操作所需的参数。
     * @return TableStore服务返回的结果
     * @throws TableStoreException    TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public BatchWriteRowResponse batchWriteRow(BatchWriteRowRequest batchWriteRowRequest) 
    		throws TableStoreException, ClientException;

    /**
     * 从表中查询一个范围内的多行数据。
     *
     * @param getRangeRequest 执行GetRange操作所需的参数。
     * @return TableStore服务返回的结果
     * @throws TableStoreException    TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public GetRangeResponse getRange(GetRangeRequest getRangeRequest)
            throws TableStoreException, ClientException;
    
    /**
     * 对表的数据根据一定的数据大小进行分块，并返回分块的信息以供数据获取接口使用。返回的数据分块按照主键列的递增顺序排列，返回的每个数据分块信息中包含分块所处的partition分区的ID的哈希值以及起始行和终止行的主键值，遵循左闭右开区间。
     *
     * @param computeSplitsBySizeRequest 执行ComputeSplitsBySize操作所需的参数。
     * @return TableStore服务返回的结果
     * @throws TableStoreException    TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public ComputeSplitsBySizeResponse computeSplitsBySize(ComputeSplitsBySizeRequest computeSplitsBySizeRequest)
            throws TableStoreException, ClientException;

    /**
     * 以迭代器的形式封装{@link #getRange(GetRangeRequest)}接口。
     *
     * @param rangeIteratorParameter 执行createRangeIterator操作所需的参数。
     * @return 行的迭代器
     * @throws TableStoreException    TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public Iterator<Row> createRangeIterator(
            RangeIteratorParameter rangeIteratorParameter) throws TableStoreException,
            ClientException;

    public WideColumnIterator createWideColumnIterator(GetRowRequest getRowRequest) throws TableStoreException,
            ClientException;

    /**
     * 获取用户当前实例下的全部Stream列表或者特定表下的Stream。
     *
     * @param listStreamRequest 执行ListStream操作所需的参数。
     * @return TableStore服务返回的结果
     * @throws TableStoreException    TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public ListStreamResponse listStream(ListStreamRequest listStreamRequest)
            throws TableStoreException, ClientException;

    /**
     * 获取指定Stream的详细信息。通过此方法获取Shard列表。
     *
     * @param describeStreamRequest 执行DescribeStream操作所需的参数。
     * @return TableStore服务返回的结果
     * @throws TableStoreException    TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public DescribeStreamResponse describeStream(DescribeStreamRequest describeStreamRequest)
            throws TableStoreException, ClientException;

    /**
     * 获取ShardIterator，可通过ShardIterator读取Shard中的数据。
     *
     * @param getShardIteratorRequest 执行GetShardIterator操作所需的参数。
     * @return TableStore服务返回的结果
     * @throws TableStoreException    TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public GetShardIteratorResponse getShardIterator(GetShardIteratorRequest getShardIteratorRequest)
            throws TableStoreException, ClientException;

    /**
     * 通过ShardIterator读取Shard中的数据。
     *
     * @param getStreamRecordRequest 执行GetStreamRecord操作所需的参数。
     * @return TableStore服务返回的结果
     * @throws TableStoreException    TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public GetStreamRecordResponse getStreamRecord(GetStreamRecordRequest getStreamRecordRequest)
            throws TableStoreException, ClientException;

    /**
     * 创建SearchIndex
     * @param request  创建SearchIndex所需的参数，详见{@link CreateSearchIndexRequest}
     * @return SearchIndex服务返回的创建结果
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public CreateSearchIndexResponse createSearchIndex(CreateSearchIndexRequest request)
            throws TableStoreException, ClientException;

    /**
     * 获取表下的SearchIndex列表
     * <p>一个table下面，可以存在多个SearchIndex表，通过该函数，将能够获取一个table下面的所有SearchIndex信息</p>
     * @param request  获取SearchIndex列表所需的参数
     * @return  TableStore指定表下的SearchIndex列表
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public ListSearchIndexResponse listSearchIndex(ListSearchIndexRequest request)
            throws TableStoreException, ClientException;

    /**
     * 删除SearchIndex
     * <p>通过指定 tableName 和 indexName 即可删除一个index </p>
     * <p>提示：在没有删除一个table下面所有的index之前，是不允许删除table的</p>
     * @param request  删除SearchIndex所需的参数
     * @return 删除SearchIndex服务执行后返回的删除结果
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public DeleteSearchIndexResponse deleteSearchIndex(DeleteSearchIndexRequest request)
            throws TableStoreException, ClientException;

    /**
     * 获取一个SearchIndex的信息
     * @param request  获取SearchIndex所需的参数（tableName 和 indexName）
     * @return 返回指定index的 schema 和目前的同步状态信息
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public DescribeSearchIndexResponse describeSearchIndex(DescribeSearchIndexRequest request)
            throws TableStoreException, ClientException;

    /**
     * 搜索功能
     * <p>构建自己的SearchRequest，然后获取SearchResponse</p>
     * <p>示例：</p>
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
     * @param request  进行搜索所需的参数，详见{@link SearchRequest}
     * @return  搜索结果，详见{@link SearchResponse}
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public SearchResponse search(SearchRequest request)
            throws TableStoreException, ClientException;

    /**
     * 开启一个本地事务
     * @param request  启动本地事务操作所需的参数
     * @return
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public StartLocalTransactionResponse startLocalTransaction(StartLocalTransactionRequest request)
            throws TableStoreException, ClientException;

    /**
     * 提交一个事务
     * @param request  提交事务操作所需的参数
     * @return
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public CommitTransactionResponse commitTransaction(CommitTransactionRequest request)
            throws TableStoreException, ClientException;

    /**
     * 取消一个事务
     * @param request  取消事务操作所需的参数
     * @return
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    public AbortTransactionResponse abortTransaction(AbortTransactionRequest request)
            throws TableStoreException, ClientException;

    /**
     * 转换成异步接口的Client。
     * @return  异步接口Client。
     */
    public AsyncClientInterface asAsyncClient();

    /**
     * 释放资源。
     * <p>请确保在所有请求执行完毕之后释放资源。释放资源之后将不能再发送请求，正在执行的请求可能无法返回结果。</p>
     */
    public void shutdown();

    /**
     * Switch CredentialsProvider。
     *
     * @param newCrdsProvider new CredentialsProvider, see {@link com.alicloud.openservices.tablestore.core.auth.CredentialsProviderFactory}.
     */
    public void switchCredentialsProvider(CredentialsProvider newCrdsProvider);
}
