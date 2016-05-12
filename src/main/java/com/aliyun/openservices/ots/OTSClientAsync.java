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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.aliyun.openservices.ots.internal.OTSUtil.OTS_RESOURCE_MANAGER;
import static com.aliyun.openservices.ots.utils.CodingUtils.assertStringNotNullOrEmpty;

/**
 * 访问阿里云开放结构化数据服务（Open Table Service, OTS）的入口类。
 */
public class OTSClientAsync implements OTSAsync {
    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime()
            .availableProcessors();
    private String endpoint; // OTS endpoint
    private String instanceName; // 实例的名称
    private ServiceCredentials credentials; // 用户身份信息。
    private ServiceClient client;
    private ScheduledExecutorService retryExecutor;
    private ExecutorService callbackExecutor; // 用于执行Callback
    private OTSServiceConfiguration serviceConfig;
    private OTSAsyncTableOperation asyncTableOp;
    private OTSAsyncDataOperation asyncDataOp;
    private OTSRetryStrategy retryStrategy;
    private Random random = new Random();

    /**
     * 使用指定的OTS Endpoint和默认配置构造一个新的{@link OTSClientAsync}实例。
     *
     * 注意：
     * 1. OTSClientAsync提供了访问OTS的异步接口，异步接口支持指定Callback，同时会返回Future。
     * 2. 大多数情况下，全局创建一个OTSClientAsync对象即可(线程安全)，不需要每次创建一个。
     *    当并发极高时（数万QPS），可以尝试使用多个OTSClientAsync对象进行性能测试与调优，但数目也不宜过多。
     * 3. 每个OTSClientAsync会占用一定的线程、连接资源，可以通过ClientConfiguration配置线程数、连接数等。
     * 4. 在使用完毕后，请调用shutdown方法释放OTSClientAsync占有的线程和连接资源。
     *
     * @param endpoint        OTS服务的endpoint。
     * @param accessKeyId     访问OTS服务的Access ID。
     * @param accessKeySecret 访问OTS服务的Access Key。
     */
    public OTSClientAsync(String endpoint, String accessKeyId,
                          String accessKeySecret, String instanceName) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, null, null, null);
    }

    /**
     * 使用指定的OTS Endpoint和默认配置构造一个新的{@link OTSClientAsync}实例。
     *
     * 注意：
     * 1. OTSClientAsync提供了访问OTS的异步接口，异步接口支持指定Callback，同时会返回Future。
     * 2. 大多数情况下，全局创建一个OTSClientAsync对象即可(线程安全)，不需要每次创建一个。
     *    当并发极高时（数万QPS），可以尝试使用多个OTSClientAsync对象进行性能测试与调优，但数目也不宜过多。
     * 3. 每个OTSClientAsync会占用一定的线程、连接资源，可以通过ClientConfiguration配置线程数、连接数等。
     * 4. 在使用完毕后，请调用shutdown方法释放OTSClientAsync占有的线程和连接资源。
     *
     * @param endpoint        OTS服务的endpoint。
     * @param accessKeyId     访问OTS服务的Access ID。
     * @param accessKeySecret 访问OTS服务的Access Key。
     * @param stsToken        OTS服务短期访问凭证。详情参考阿里云STS服务文档。
     */
    public OTSClientAsync(String endpoint, String accessKeyId,
                          String accessKeySecret, String instanceName, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, null, null, null, stsToken);
    }

    /**
     * 使用指定的OTS Endpoint和配置构造一个新的{@link OTSClientAsync}实例。
     *
     * 注意：
     * 1. OTSClientAsync提供了访问OTS的异步接口，异步接口支持指定Callback，同时会返回Future。
     * 2. 大多数情况下，全局创建一个OTSClientAsync对象即可(线程安全)，不需要每次创建一个。
     *    当并发极高时（数万QPS），可以尝试使用多个OTSClientAsync对象进行性能测试与调优，但数目也不宜过多。
     * 3. 每个OTSClientAsync会占用一定的线程、连接资源，可以通过ClientConfiguration配置线程数、连接数等。
     * 4. 在使用完毕后，请调用shutdown方法释放OTSClientAsync占有的线程和连接资源。
     *
     * @param endpoint        OTS服务的endpoint。
     * @param accessKeyId     访问OTS服务的Access ID。
     * @param accessKeySecret 访问OTS服务的Access Key。
     * @param config          客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param otsConfig       OTS服务相关配置信息（{@link OTSServiceConfiguration}）。 如果传入null则使用默认配置。
     */
    public OTSClientAsync(String endpoint, String accessKeyId,
                          String accessKeySecret, String instanceName,
                          ClientConfiguration config, OTSServiceConfiguration otsConfig) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config,
                otsConfig, null);
    }

    public OTSClientAsync(String endpoint, String accessKeyId,
                          String accessKeySecret, String instanceName,
                          ClientConfiguration config) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config,
                null, null);
    }

    /**
     * 使用指定的OTS Endpoint和配置构造一个新的{@link OTSClientAsync}实例。
     *
     * 注意：
     * 1. OTSClientAsync提供了访问OTS的异步接口，异步接口支持指定Callback，同时会返回Future。
     * 2. 大多数情况下，全局创建一个OTSClientAsync对象即可(线程安全)，不需要每次创建一个。
     *    当并发极高时（数万QPS），可以尝试使用多个OTSClientAsync对象进行性能测试与调优，但数目也不宜过多。
     * 3. 每个OTSClientAsync会占用一定的线程、连接资源，可以通过ClientConfiguration配置线程数、连接数等。
     * 4. 在使用完毕后，请调用shutdown方法释放OTSClientAsync占有的线程和连接资源。
     *
     * @param endpoint         OTS服务的endpoint。
     * @param accessKeyId      访问OTS服务的Access ID。
     * @param accessKeySecret  访问OTS服务的Access Key。
     * @param config           客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param otsConfig        OTS服务相关配置信息（{@link OTSServiceConfiguration}）。 如果传入null则使用默认配置。
     * @param callbackExecutor 用于执行用户在调用OTSAsync接口时传入的Callback。如果传入null则使用默认配置(
     *                         线程数与CPU核数相同的线程池)。
     */
    public OTSClientAsync(String endpoint, String accessKeyId,
                          String accessKeySecret, String instanceName,
                          ClientConfiguration config, OTSServiceConfiguration otsConfig,
                          ExecutorService callbackExecutor) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, otsConfig, callbackExecutor, null);
    }

    /**
     * 使用指定的OTS Endpoint和配置构造一个新的{@link OTSClientAsync}实例。
     *
     * 注意：
     * 1. OTSClientAsync提供了访问OTS的异步接口，异步接口支持指定Callback，同时会返回Future。
     * 2. 大多数情况下，全局创建一个OTSClientAsync对象即可(线程安全)，不需要每次创建一个。
     *    当并发极高时（数万QPS），可以尝试使用多个OTSClientAsync对象进行性能测试与调优，但数目也不宜过多。
     * 3. 每个OTSClientAsync会占用一定的线程、连接资源，可以通过ClientConfiguration配置线程数、连接数等。
     * 4. 在使用完毕后，请调用shutdown方法释放OTSClientAsync占有的线程和连接资源。
     *
     * @param endpoint         OTS服务的endpoint。
     * @param accessKeyId      访问OTS服务的Access ID。
     * @param accessKeySecret  访问OTS服务的Access Key。
     * @param config           客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param otsConfig        OTS服务相关配置信息（{@link OTSServiceConfiguration}）。 如果传入null则使用默认配置。
     * @param callbackExecutor 用于执行用户在调用OTSAsync接口时传入的Callback。如果传入null则使用默认配置(
     *                         线程数与CPU核数相同的线程池)。
     * @param stsToken         OTS服务短期访问凭证。详情参考阿里云STS服务文档。
     */
    public OTSClientAsync(String endpoint, String accessKeyId,
                          String accessKeySecret, String instanceName,
                          ClientConfiguration config, OTSServiceConfiguration otsConfig,
                          ExecutorService callbackExecutor, String stsToken) {
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
        if (callbackExecutor != null) {
            this.callbackExecutor = callbackExecutor;
        } else {
            this.callbackExecutor = Executors
                    .newFixedThreadPool(AVAILABLE_PROCESSORS);
        }

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
        this.asyncDataOp.setExtraHeaders(extraHeaders);
        this.asyncTableOp.setExtraHeaders(extraHeaders);
    }

    /**
     * 这是OTSBasicFuture的callback, 用来调用用户的回调函数。
     */
    private <Req, Res> OTSFutureCallback<Res> buildOTSBasicFutureCallback(
            final Req request, final OTSCallback<Req, Res> otsCallback) {

        // 只有OTSClientAsync有callbackExecutor, otsCallback可能为null.
        if (otsCallback == null) {
            return null;
        }

        return new OTSFutureCallback<Res>() {
            @Override
            public void completed(Res result) {
                final OTSContext<Req, Res> otsContext = new OTSContext<Req, Res>(
                        request, result);
                callbackExecutor.submit(new Runnable() {
                    public void run() {
                        otsCallback.onCompleted(otsContext);
                    }
                });
            }

            @Override
            public void failed(final Exception ex) {
                final OTSContext<Req, Res> otsContext = new OTSContext<Req, Res>(
                        request, null);
                if (ex instanceof OTSException) {
                    callbackExecutor.submit(new Runnable() {
                        public void run() {
                            otsCallback.onFailed(otsContext, (OTSException) ex);
                        }
                    });
                } else if (ex instanceof ClientException) {
                    callbackExecutor.submit(new Runnable() {
                        public void run() {
                            otsCallback.onFailed(otsContext,
                                    (ClientException) ex);
                        }
                    });
                }
            }
        };
    }

    @Override
    public OTSFuture<ListTableResult> listTable() throws ClientException {
        return listTable(null);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTSAsync#listTable(com.aliyun.openservices
     * .ots.core.OTSCallback)
     */
    @Override
    public OTSFuture<ListTableResult> listTable(
            OTSCallback<ListTableRequest, ListTableResult> callback)
            throws ClientException {
        OTSBasicFuture<ListTableResult> future = new OTSBasicFuture<ListTableResult>(buildOTSBasicFutureCallback(null, callback));

        OTSExecutionContext<ListTableRequest, ListTableResult> executionContext =
                new OTSExecutionContext<ListTableRequest, ListTableResult>(null, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_LIST_TABLE, executionContext));
        executionContext.setCallable(new ListTableCallable(this.getAsyncTableOperation(), executionContext));
        executionContext.getCallable().call();
        return future;
    }

    @Override
    public OTSFuture<CreateTableResult> createTable(CreateTableRequest createTableRequest) throws ClientException {
        return createTable(createTableRequest, null);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTSAsync#createTable(com.aliyun.openservices
     * .ots.model.CreateTableRequest,
     * com.aliyun.openservices.ots.core.OTSCallback)
     */
    @Override
    public OTSFuture<CreateTableResult> createTable(
            CreateTableRequest createTableRequest,
            OTSCallback<CreateTableRequest, CreateTableResult> callback)
            throws ClientException {
        Preconditions.checkNotNull(createTableRequest);

        OTSBasicFuture<CreateTableResult> future = new OTSBasicFuture<CreateTableResult>(buildOTSBasicFutureCallback(createTableRequest, callback));

        OTSExecutionContext<CreateTableRequest, CreateTableResult> executionContext =
                new OTSExecutionContext<CreateTableRequest, CreateTableResult>(createTableRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_CREATE_TABLE, executionContext));
        executionContext.setCallable(new CreateTableCallable(this.getAsyncTableOperation(), executionContext));
        executionContext.getCallable().call();
        return future;
    }

    @Override
    public OTSFuture<DescribeTableResult> describeTable(DescribeTableRequest describeTableRequest) throws ClientException {
        return describeTable(describeTableRequest, null);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTSAsync#describeTable(com.aliyun.openservices
     * .ots.model.DescribeTableRequest,
     * com.aliyun.openservices.ots.core.OTSCallback)
     */
    @Override
    public OTSFuture<DescribeTableResult> describeTable(
            DescribeTableRequest describeTableRequest,
            OTSCallback<DescribeTableRequest, DescribeTableResult> callback)
            throws ClientException {
        Preconditions.checkNotNull(describeTableRequest);

        OTSBasicFuture<DescribeTableResult> future = new OTSBasicFuture<DescribeTableResult>(buildOTSBasicFutureCallback(describeTableRequest, callback));

        OTSExecutionContext<DescribeTableRequest, DescribeTableResult> executionContext =
                new OTSExecutionContext<DescribeTableRequest, DescribeTableResult>(describeTableRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_DESCRIBE_TABLE, executionContext));
        executionContext.setCallable(new DescribeTableCallable(this.getAsyncTableOperation(), executionContext));
        executionContext.getCallable().call();
        return future;
    }

    @Override
    public OTSFuture<DeleteTableResult> deleteTable(DeleteTableRequest deleteTableRequest) throws ClientException {
        return deleteTable(deleteTableRequest, null);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTSAsync#deleteTable(com.aliyun.openservices
     * .ots.model.DeleteTableRequest,
     * com.aliyun.openservices.ots.core.OTSCallback)
     */
    @Override
    public OTSFuture<DeleteTableResult> deleteTable(
            DeleteTableRequest deleteTableRequest,
            OTSCallback<DeleteTableRequest, DeleteTableResult> callback)
            throws ClientException {
        Preconditions.checkNotNull(deleteTableRequest);

        OTSBasicFuture<DeleteTableResult> future = new OTSBasicFuture<DeleteTableResult>(buildOTSBasicFutureCallback(deleteTableRequest, callback));

        OTSExecutionContext<DeleteTableRequest, DeleteTableResult> executionContext =
                new OTSExecutionContext<DeleteTableRequest, DeleteTableResult>(deleteTableRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_DELETE_TABLE, executionContext));
        executionContext.setCallable(new DeleteTableCallable(this.getAsyncTableOperation(), executionContext));
        executionContext.getCallable().call();
        return future;
    }

    @Override
    public OTSFuture<UpdateTableResult> updateTable(UpdateTableRequest updateTableRequest) throws ClientException {
        return updateTable(updateTableRequest, null);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTSAsync#updateTable(com.aliyun.openservices
     * .ots.model.UpdateTableRequest,
     * com.aliyun.openservices.ots.core.OTSCallback)
     */
    @Override
    public OTSFuture<UpdateTableResult> updateTable(
            UpdateTableRequest updateTableRequest,
            OTSCallback<UpdateTableRequest, UpdateTableResult> callback)
            throws ClientException {
        Preconditions.checkNotNull(updateTableRequest);

        OTSBasicFuture<UpdateTableResult> future = new OTSBasicFuture<UpdateTableResult>(buildOTSBasicFutureCallback(updateTableRequest, callback));

        OTSExecutionContext<UpdateTableRequest, UpdateTableResult> executionContext =
                new OTSExecutionContext<UpdateTableRequest, UpdateTableResult>(updateTableRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_UPDATE_TABLE, executionContext));
        executionContext.setCallable(new UpdateTableCallable(this.getAsyncTableOperation(), executionContext));
        executionContext.getCallable().call();
        return future;
    }

    @Override
    public OTSFuture<GetRowResult> getRow(GetRowRequest getRowRequest) throws ClientException {
        return getRow(getRowRequest, null);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTSAsync#getRow(com.aliyun.openservices.ots
     * .model.GetRowRequest, com.aliyun.openservices.ots.core.OTSCallback)
     */
    @Override
    public OTSFuture<GetRowResult> getRow(GetRowRequest getRowRequest,
                                          OTSCallback<GetRowRequest, GetRowResult> callback)
            throws ClientException {
        Preconditions.checkNotNull(getRowRequest);

        OTSBasicFuture<GetRowResult> future = new OTSBasicFuture<GetRowResult>(buildOTSBasicFutureCallback(getRowRequest, callback));

        OTSExecutionContext<GetRowRequest, GetRowResult> executionContext =
                new OTSExecutionContext<GetRowRequest, GetRowResult>(getRowRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_GET_ROW, executionContext));
        executionContext.setCallable(new GetRowCallable(this.getAsyncDataOperation(), executionContext));
        executionContext.getCallable().call();
        return future;
    }

    @Override
    public OTSFuture<PutRowResult> putRow(PutRowRequest putRowRequest) throws ClientException {
        return putRow(putRowRequest, null);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTSAsync#putRow(com.aliyun.openservices.ots
     * .model.PutRowRequest, com.aliyun.openservices.ots.core.OTSCallback)
     */
    @Override
    public OTSFuture<PutRowResult> putRow(PutRowRequest putRowRequest,
                                          OTSCallback<PutRowRequest, PutRowResult> callback)
            throws ClientException {
        Preconditions.checkNotNull(putRowRequest);

        OTSBasicFuture<PutRowResult> future = new OTSBasicFuture<PutRowResult>(buildOTSBasicFutureCallback(putRowRequest, callback));

        OTSExecutionContext<PutRowRequest, PutRowResult> executionContext =
                new OTSExecutionContext<PutRowRequest, PutRowResult>(putRowRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_PUT_ROW, executionContext));
        executionContext.setCallable(new PutRowCallable(this.getAsyncDataOperation(), executionContext));
        executionContext.getCallable().call();
        return future;
    }

    @Override
    public OTSFuture<UpdateRowResult> updateRow(UpdateRowRequest updateRowRequest) throws ClientException {
        return updateRow(updateRowRequest, null);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTSAsync#updateRow(com.aliyun.openservices
     * .ots.model.UpdateRowRequest,
     * com.aliyun.openservices.ots.core.OTSCallback)
     */
    @Override
    public OTSFuture<UpdateRowResult> updateRow(
            UpdateRowRequest updateRowRequest,
            OTSCallback<UpdateRowRequest, UpdateRowResult> callback)
            throws ClientException {
        Preconditions.checkNotNull(updateRowRequest);

        OTSBasicFuture<UpdateRowResult> future = new OTSBasicFuture<UpdateRowResult>(buildOTSBasicFutureCallback(updateRowRequest, callback));

        OTSExecutionContext<UpdateRowRequest, UpdateRowResult> executionContext =
                new OTSExecutionContext<UpdateRowRequest, UpdateRowResult>(updateRowRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_UPDATE_ROW, executionContext));
        executionContext.setCallable(new UpdateRowCallable(this.getAsyncDataOperation(), executionContext));
        executionContext.getCallable().call();
        return future;
    }

    @Override
    public OTSFuture<DeleteRowResult> deleteRow(DeleteRowRequest deleteRowRequest) throws ClientException {
        return deleteRow(deleteRowRequest, null);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTSAsync#deleteRow(com.aliyun.openservices
     * .ots.model.DeleteRowRequest,
     * com.aliyun.openservices.ots.core.OTSCallback)
     */
    @Override
    public OTSFuture<DeleteRowResult> deleteRow(
            DeleteRowRequest deleteRowRequest,
            OTSCallback<DeleteRowRequest, DeleteRowResult> callback)
            throws ClientException {
        Preconditions.checkNotNull(deleteRowRequest);

        OTSBasicFuture<DeleteRowResult> future = new OTSBasicFuture<DeleteRowResult>(buildOTSBasicFutureCallback(deleteRowRequest, callback));

        OTSExecutionContext<DeleteRowRequest, DeleteRowResult> executionContext =
                new OTSExecutionContext<DeleteRowRequest, DeleteRowResult>(deleteRowRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_DELETE_ROW, executionContext));
        executionContext.setCallable(new DeleteRowCallable(this.getAsyncDataOperation(), executionContext));
        executionContext.getCallable().call();
        return future;
    }

    @Override
    public OTSFuture<BatchGetRowResult> batchGetRow(BatchGetRowRequest batchGetRowRequest) throws ClientException {
        return batchGetRow(batchGetRowRequest, null);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTSAsync#batchGetRow(com.aliyun.openservices
     * .ots.model.BatchGetRowRequest,
     * com.aliyun.openservices.ots.core.OTSCallback)
     */
    @Override
    public OTSFuture<BatchGetRowResult> batchGetRow(
            BatchGetRowRequest batchGetRowRequest,
            OTSCallback<BatchGetRowRequest, BatchGetRowResult> callback)
            throws ClientException {
        Preconditions.checkNotNull(batchGetRowRequest);

        OTSBasicFuture<BatchGetRowResult> future = new OTSBasicFuture<BatchGetRowResult>(buildOTSBasicFutureCallback(batchGetRowRequest, callback));

        BatchGetRowExecutionContext executionContext =
                new BatchGetRowExecutionContext(batchGetRowRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_BATCH_GET_ROW, executionContext));
        executionContext.setCallable(new BatchGetRowCallable(this.getAsyncDataOperation(), executionContext));
        executionContext.getCallable().call();
        return future;
    }

    @Override
    public OTSFuture<BatchWriteRowResult> batchWriteRow(BatchWriteRowRequest batchWriteRowRequest) throws ClientException {
        return batchWriteRow(batchWriteRowRequest, null);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTSAsync#batchWriteRow(com.aliyun.openservices
     * .ots.model.BatchWriteRowRequest,
     * com.aliyun.openservices.ots.core.OTSCallback)
     */
    @Override
    public OTSFuture<BatchWriteRowResult> batchWriteRow(
            BatchWriteRowRequest batchWriteRowRequest,
            OTSCallback<BatchWriteRowRequest, BatchWriteRowResult> callback)
            throws ClientException {
        Preconditions.checkNotNull(batchWriteRowRequest);

        OTSBasicFuture<BatchWriteRowResult> future = new OTSBasicFuture<BatchWriteRowResult>(buildOTSBasicFutureCallback(batchWriteRowRequest, callback));

        BatchWriteRowExecutionContext executionContext =
                new BatchWriteRowExecutionContext(batchWriteRowRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_BATCH_WRITE_ROW, executionContext));
        executionContext.setCallable(new BatchWriteRowCallable(this.getAsyncDataOperation(), executionContext));
        executionContext.getCallable().call();
        return future;
    }

    @Override
    public OTSFuture<GetRangeResult> getRange(GetRangeRequest getRangeRequest) throws ClientException {
        return getRange(getRangeRequest, null);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * OTSAsync#getRange(com.aliyun.openservices
     * .ots.model.GetRangeRequest,
     * com.aliyun.openservices.ots.core.OTSCallback)
     */
    @Override
    public OTSFuture<GetRangeResult> getRange(GetRangeRequest getRangeRequest,
                                              OTSCallback<GetRangeRequest, GetRangeResult> callback)
            throws ClientException {
        Preconditions.checkNotNull(getRangeRequest);

        OTSBasicFuture<GetRangeResult> future = new OTSBasicFuture<GetRangeResult>(buildOTSBasicFutureCallback(getRangeRequest, callback));

        OTSExecutionContext<GetRangeRequest, GetRangeResult> executionContext =
                new OTSExecutionContext<GetRangeRequest, GetRangeResult>(getRangeRequest, future, getOTSTraceLogger(), retryStrategy, retryExecutor);

        executionContext.setAsyncClientCallback(new AsyncClientFutureCallback(OTSActionNames.ACTION_GET_RANGE, executionContext));
        executionContext.setCallable(new GetRangeCallable(this.getAsyncDataOperation(), executionContext));
        executionContext.getCallable().call();
        return future;
    }

    @Override
    public void shutdown() {
        this.retryExecutor.shutdown();
        this.callbackExecutor.shutdown();
        this.client.shutdown();
    }
}
