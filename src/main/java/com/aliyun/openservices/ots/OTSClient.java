/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 *
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots;

import com.aliyun.openservices.ots.auth.ServiceCredentials;
import com.aliyun.openservices.ots.comm.AsyncClientFutureCallback;
import com.aliyun.openservices.ots.comm.AsyncServiceClient;
import com.aliyun.openservices.ots.comm.ServiceClient;
import com.aliyun.openservices.ots.internal.*;
import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.utils.Preconditions;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.aliyun.openservices.ots.internal.OTSUtil.OTS_RESOURCE_MANAGER;
import static com.aliyun.openservices.ots.utils.CodingUtils.assertStringNotNullOrEmpty;

/**
 * 访问阿里云开放结构化数据服务（Open Table Service, OTS）的入口类。
 */
public class OTSClient implements OTS {
    private String endpoint; // OTS endpoint
    private String instanceName; // 实例的名称
    private ServiceCredentials credentials; // 用户身份信息。
    private ServiceClient client;
    private ScheduledExecutorService retryExecutor;
    private OTSServiceConfiguration serviceConfig;
    private OTSAsyncTableOperation asyncTableOp;
    private OTSAsyncDataOperation asyncDataOp;
    private OTSRetryStrategy retryStrategy;
    private Random random = new Random();

    /**
     * 使用指定的OTS Endpoint和默认配置构造一个新的{@link OTSClient}实例。
     *
     * 注意：
     * 1. 大多数情况下，全局创建一个OTSClient对象即可(线程安全)，不需要每次创建一个。
     *    当并发极高时（数万QPS），可以尝试使用多个OTSClient对象进行性能测试与调优，但数目也不宜过多。
     * 2. 每个OTSClient会占用一定的线程、连接资源，可以通过ClientConfiguration配置线程数、连接数等。
     * 3. 在使用完毕后，请调用shutdown方法释放OTSClient占有的线程和连接资源。
     *
     * @param endpoint        OTS服务的endpoint。
     * @param accessKeyId     访问OTS服务的Access ID。
     * @param accessKeySecret 访问OTS服务的Access Key。
     */
    public OTSClient(String endpoint, String accessKeyId,
                     String accessKeySecret, String instanceName) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, null, null);
    }

    /**
     * 使用指定的OTS Endpoint和默认配置构造一个新的{@link OTSClient}实例。
     *
     * 注意：
     * 1. 大多数情况下，全局创建一个OTSClient对象即可(线程安全)，不需要每次创建一个。
     *    当并发极高时（数万QPS），可以尝试使用多个OTSClient对象进行性能测试与调优，但数目也不宜过多。
     * 2. 每个OTSClient会占用一定的线程、连接资源，可以通过ClientConfiguration配置线程数、连接数等。
     * 3. 在使用完毕后，请调用shutdown方法释放OTSClient占有的线程和连接资源。
     *
     * @param endpoint        OTS服务的endpoint。
     * @param accessKeyId     访问OTS服务的Access ID。
     * @param accessKeySecret 访问OTS服务的Access Key。
     * @param stsToken        OTS服务短期访问凭证。详情参考阿里云STS服务文档。
     */
    public OTSClient(String endpoint, String accessKeyId,
                     String accessKeySecret, String instanceName, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, null, null, stsToken);
    }

    /**
     * 使用指定的OTS Endpoint和配置构造一个新的{@link OTSClient}实例。
     *
     * 注意：
     * 1. 大多数情况下，全局创建一个OTSClient对象即可(线程安全)，不需要每次创建一个。
     *    当并发极高时（数万QPS），可以尝试使用多个OTSClient对象进行性能测试与调优，但数目也不宜过多。
     * 2. 每个OTSClient会占用一定的线程、连接资源，可以通过ClientConfiguration配置线程数、连接数等。
     * 3. 在使用完毕后，请调用shutdown方法释放OTSClient占有的线程和连接资源。
     *
     * @param endpoint        OTS服务的endpoint。
     * @param accessKeyId     访问OTS服务的Access ID。
     * @param accessKeySecret 访问OTS服务的Access Key。
     * @param config          客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     */
    public OTSClient(String endpoint, String accessKeyId,
                     String accessKeySecret, String instanceName,
                     ClientConfiguration config) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, null);
    }

    /**
     * 使用指定的OTS Endpoint和配置构造一个新的{@link OTSClient}实例。
     *
     * 注意：
     * 1. 大多数情况下，全局创建一个OTSClient对象即可(线程安全)，不需要每次创建一个。
     *    当并发极高时（数万QPS），可以尝试使用多个OTSClient对象进行性能测试与调优，但数目也不宜过多。
     * 2. 每个OTSClient会占用一定的线程、连接资源，可以通过ClientConfiguration配置线程数、连接数等。
     * 3. 在使用完毕后，请调用shutdown方法释放OTSClient占有的线程和连接资源。
     *
     * @param endpoint        OTS服务的endpoint。
     * @param accessKeyId     访问OTS服务的Access ID。
     * @param accessKeySecret 访问OTS服务的Access Key。
     * @param config          客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param otsConfig       OTS服务相关配置信息（{@link OTSServiceConfiguration}）。 如果传入null则使用默认配置。
     */
    public OTSClient(String endpoint, String accessKeyId,
                     String accessKeySecret, String instanceName,
                     ClientConfiguration config, OTSServiceConfiguration otsConfig) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, otsConfig, null);
    }

    /**
     * 使用指定的OTS Endpoint和配置构造一个新的{@link OTSClient}实例。
     *
     * 注意：
     * 1. 大多数情况下，全局创建一个OTSClient对象即可(线程安全)，不需要每次创建一个。
     *    当并发极高时（数万QPS），可以尝试使用多个OTSClient对象进行性能测试与调优，但数目也不宜过多。
     * 2. 每个OTSClient会占用一定的线程、连接资源，可以通过ClientConfiguration配置线程数、连接数等。
     * 3. 在使用完毕后，请调用shutdown方法释放OTSClient占有的线程和连接资源。
     *
     * @param endpoint        OTS服务的endpoint。
     * @param accessKeyId     访问OTS服务的Access ID。
     * @param accessKeySecret 访问OTS服务的Access Key。
     * @param config          客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param otsConfig       OTS服务相关配置信息（{@link OTSServiceConfiguration}）。 如果传入null则使用默认配置。
     * @param stsToken        OTS服务短期访问凭证。详情参考阿里云STS服务文档。
     */
    public OTSClient(String endpoint, String accessKeyId,
                     String accessKeySecret, String instanceName,
                     ClientConfiguration config, OTSServiceConfiguration otsConfig, String stsToken) {
        assertStringNotNullOrEmpty(endpoint, "endpoint");
        assertStringNotNullOrEmpty(accessKeyId, "accessKeyId");
        assertStringNotNullOrEmpty(accessKeySecret, "accessKeySecret");
        assertStringNotNullOrEmpty(instanceName, "instanceName");

        try {
            Preconditions.checkArgument(instanceName.length() == instanceName.getBytes(OTSConsts.DEFAULT_ENCODING).length,
                    "InstanceName should not have multibyte character.");
        } catch (UnsupportedEncodingException ex) {
            throw new ClientException("UnsupportedEncoding", ex);
        }

        if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
            throw new IllegalArgumentException(
                    OTS_RESOURCE_MANAGER
                            .getString("EndpointProtocolInvalid"));
        }

        this.endpoint = endpoint;

        this.credentials = new ServiceCredentials(accessKeyId, accessKeySecret, stsToken);

        if (config == null) {
            config = new ClientConfiguration();
        }

        this.client = new AsyncServiceClient(config);

        this.retryExecutor = Executors.newScheduledThreadPool(config
                .getRetryThreadCount());

        this.serviceConfig = otsConfig != null ? otsConfig
                : new OTSServiceConfiguration();

        this.retryStrategy = this.serviceConfig.getRetryStrategy();

        this.instanceName = instanceName;

        asyncTableOp = new OTSAsyncTableOperation(this.endpoint, this.instanceName, this.client,
                this.credentials, this.serviceConfig);

        asyncDataOp = new OTSAsyncDataOperation(this.endpoint, this.instanceName, this.client,
                this.credentials, this.serviceConfig);
    }

    /**
     * 返回访问的OTS Endpoint。
     *
     * @return OTS Endpoint。
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * 返回访问的实例的名称
     *
     * @return instance name
     */
    public String getInstanceName() {
        return instanceName;
    }

    private OTSAsyncTableOperation getAsyncTableOperation() {
        return asyncTableOp;
    }

    private OTSAsyncDataOperation getAsyncDataOperation() {
        return asyncDataOp;
    }

    private OTSTraceLogger getOTSTraceLogger() {
        String traceId = new UUID(random.nextLong(), new Random().nextLong()).toString();
        OTSTraceLogger traceLogger = new OTSTraceLogger(traceId,
                this.serviceConfig.getTimeThresholdOfTraceLogger());
        return traceLogger;
    }

    public void setExtraHeaders(Map<String, String> extraHeaders) {
        this.asyncTableOp.setExtraHeaders(extraHeaders);
        this.asyncDataOp.setExtraHeaders(extraHeaders);
    }

    /*
    * (non-Javadoc)
    *
    * @see
    * OTS#createTable(com.aliyun.openservices.ots
    * .model.CreateTableRequest)
    */
    @Override
    public CreateTableResult createTable(CreateTableRequest createTableRequest) throws OTSException, ClientException {
        Preconditions.checkNotNull(createTableRequest);

        OTSBasicFuture<CreateTableResult> future = new OTSBasicFuture<CreateTableResult>(null);

        OTSExecutionContext<CreateTableRequest, CreateTableResult> executionContext =
                new OTSExecutionContext<CreateTableRequest, CreateTableResult>(createTableRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_CREATE_TABLE, executionContext));
        executionContext.setCallable(new CreateTableCallable(this.getAsyncTableOperation(), executionContext));
        executionContext.getCallable().call();
        return future.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see OTS#listTables()
     */
    @Override
    public ListTableResult listTable() throws OTSException, ClientException {
        OTSBasicFuture<ListTableResult> future = new OTSBasicFuture<ListTableResult>(null);

        OTSExecutionContext<ListTableRequest, ListTableResult> executionContext =
                new OTSExecutionContext<ListTableRequest, ListTableResult>(null, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_LIST_TABLE, executionContext));
        executionContext.setCallable(new ListTableCallable(this.getAsyncTableOperation(), executionContext));
        executionContext.getCallable().call();
        return future.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTS#describeTable(com.aliyun.openservices
     * .ots.model.DescribeTableRequest)
     */
    @Override
    public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) throws OTSException, ClientException {
        Preconditions.checkNotNull(describeTableRequest);

        OTSBasicFuture<DescribeTableResult> future = new OTSBasicFuture<DescribeTableResult>(null);

        OTSExecutionContext<DescribeTableRequest, DescribeTableResult> executionContext =
                new OTSExecutionContext<DescribeTableRequest, DescribeTableResult>(describeTableRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_DESCRIBE_TABLE, executionContext));
        executionContext.setCallable(new DescribeTableCallable(this.getAsyncTableOperation(), executionContext));
        executionContext.getCallable().call();
        return future.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTS#deleteTable(com.aliyun.openservices.ots
     * .model.DeleteTableRequest)
     */
    @Override
    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) throws OTSException, ClientException {
        Preconditions.checkNotNull(deleteTableRequest);

        OTSBasicFuture<DeleteTableResult> future = new OTSBasicFuture<DeleteTableResult>(null);

        OTSExecutionContext<DeleteTableRequest, DeleteTableResult> executionContext =
                new OTSExecutionContext<DeleteTableRequest, DeleteTableResult>(deleteTableRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_DELETE_TABLE, executionContext));
        executionContext.setCallable(new DeleteTableCallable(this.getAsyncTableOperation(), executionContext));
        executionContext.getCallable().call();
        return future.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTS#updateTableCapacity(com.aliyun.openservices
     * .ots.model.UpdateTableCapacityRequest)
     */
    @Override
    public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest) throws OTSException, ClientException {
        Preconditions.checkNotNull(updateTableRequest);

        OTSBasicFuture<UpdateTableResult> future = new OTSBasicFuture<UpdateTableResult>(null);

        OTSExecutionContext<UpdateTableRequest, UpdateTableResult> executionContext =
                new OTSExecutionContext<UpdateTableRequest, UpdateTableResult>(updateTableRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_UPDATE_TABLE, executionContext));
        executionContext.setCallable(new UpdateTableCallable(this.getAsyncTableOperation(), executionContext));
        executionContext.getCallable().call();
        return future.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTS#getRow(com.aliyun.openservices.ots.model
     * .GetRowRequest)
     */
    @Override
    public GetRowResult getRow(GetRowRequest getRowRequest) throws OTSException, ClientException {
        Preconditions.checkNotNull(getRowRequest);

        OTSBasicFuture<GetRowResult> future = new OTSBasicFuture<GetRowResult>(null);

        OTSExecutionContext<GetRowRequest, GetRowResult> executionContext =
                new OTSExecutionContext<GetRowRequest, GetRowResult>(getRowRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_GET_ROW, executionContext));
        executionContext.setCallable(new GetRowCallable(this.getAsyncDataOperation(), executionContext));
        executionContext.getCallable().call();
        return future.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTS#putRow(com.aliyun.openservices.ots.model
     * .PutRowRequest)
     */
    @Override
    public PutRowResult putRow(PutRowRequest putRowRequest) throws OTSException, ClientException {
        Preconditions.checkNotNull(putRowRequest);

        OTSBasicFuture<PutRowResult> future = new OTSBasicFuture<PutRowResult>(null);

        OTSExecutionContext<PutRowRequest, PutRowResult> executionContext =
                new OTSExecutionContext<PutRowRequest, PutRowResult>(putRowRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_PUT_ROW, executionContext));
        executionContext.setCallable(new PutRowCallable(this.getAsyncDataOperation(), executionContext));
        executionContext.getCallable().call();
        return future.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTS#updateRow(com.aliyun.openservices.ots
     * .model.UpdateRowRequest)
     */
    @Override
    public UpdateRowResult updateRow(UpdateRowRequest updateRowRequest) throws OTSException, ClientException {
        Preconditions.checkNotNull(updateRowRequest);

        OTSBasicFuture<UpdateRowResult> future = new OTSBasicFuture<UpdateRowResult>(null);

        OTSExecutionContext<UpdateRowRequest, UpdateRowResult> executionContext =
                new OTSExecutionContext<UpdateRowRequest, UpdateRowResult>(updateRowRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_UPDATE_ROW, executionContext));
        executionContext.setCallable(new UpdateRowCallable(this.getAsyncDataOperation(), executionContext));
        executionContext.getCallable().call();
        return future.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTS#deleteRow(com.aliyun.openservices.ots
     * .model.DeleteRowRequest)
     */
    @Override
    public DeleteRowResult deleteRow(DeleteRowRequest deleteRowRequest) throws OTSException, ClientException {
        Preconditions.checkNotNull(deleteRowRequest);

        OTSBasicFuture<DeleteRowResult> future = new OTSBasicFuture<DeleteRowResult>(null);

        OTSExecutionContext<DeleteRowRequest, DeleteRowResult> executionContext =
                new OTSExecutionContext<DeleteRowRequest, DeleteRowResult>(deleteRowRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_DELETE_ROW, executionContext));
        executionContext.setCallable(new DeleteRowCallable(this.getAsyncDataOperation(), executionContext));
        executionContext.getCallable().call();
        return future.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTS#batchGetRow(com.aliyun.openservices.ots
     * .model.BatchGetRowRequest)
     */
    @Override
    public BatchGetRowResult batchGetRow(final BatchGetRowRequest batchGetRowRequest) throws OTSException, ClientException {
        Preconditions.checkNotNull(batchGetRowRequest);

        OTSBasicFuture<BatchGetRowResult> future = new OTSBasicFuture<BatchGetRowResult>(null);

        BatchGetRowExecutionContext executionContext =
                new BatchGetRowExecutionContext(batchGetRowRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_BATCH_GET_ROW, executionContext));
        executionContext.setCallable(new BatchGetRowCallable(this.getAsyncDataOperation(), executionContext));
        executionContext.getCallable().call();
        return future.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTS#batchWriteRow(com.aliyun.openservices
     * .ots.model.BatchWriteRowRequest)
     */
    @Override
    public BatchWriteRowResult batchWriteRow(final BatchWriteRowRequest batchWriteRowRequest) throws OTSException, ClientException {
        Preconditions.checkNotNull(batchWriteRowRequest);

        OTSBasicFuture<BatchWriteRowResult> future = new OTSBasicFuture<BatchWriteRowResult>(null);

        BatchWriteRowExecutionContext executionContext =
                new BatchWriteRowExecutionContext(batchWriteRowRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_BATCH_WRITE_ROW, executionContext));
        executionContext.setCallable(new BatchWriteRowCallable(this.getAsyncDataOperation(), executionContext));
        executionContext.getCallable().call();
        return future.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTS#getRange(com.aliyun.openservices.ots.
     * model.GetRangeRequest)
     */
    @Override
    public GetRangeResult getRange(GetRangeRequest getRangeRequest) throws OTSException, ClientException {
        Preconditions.checkNotNull(getRangeRequest);

        OTSBasicFuture<GetRangeResult> future = new OTSBasicFuture<GetRangeResult>(null);

        OTSExecutionContext<GetRangeRequest, GetRangeResult> executionContext =
                new OTSExecutionContext<GetRangeRequest, GetRangeResult>(getRangeRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_GET_RANGE, executionContext));
        executionContext.setCallable(new GetRangeCallable(this.getAsyncDataOperation(), executionContext));
        executionContext.getCallable().call();
        return future.get();
    }

    @Override
    public Iterator<Row> createRangeIterator(
            RangeIteratorParameter rangeIteratorParameter) throws OTSException,
            ClientException {
        return new RowIterator(this, rangeIteratorParameter);
    }

    @Override
    public void shutdown() {
        this.retryExecutor.shutdown();
        this.client.shutdown();
    }

}
