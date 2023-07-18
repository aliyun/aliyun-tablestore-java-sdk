package com.alicloud.openservices.tablestore;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProviderFactory;
import com.alicloud.openservices.tablestore.core.utils.HttpUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.*;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.delivery.*;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryResponse;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.ListTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.ListTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.CheckpointRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.CheckpointResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ConnectTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ConnectTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.HeartbeatRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.HeartbeatResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ShutdownTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ShutdownTunnelResponse;
import com.google.common.cache.Cache;

public class InternalClient {
    private static int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private String endpoint; // TableStore endpoint
    private String instanceName; // 实例的名称
    private CredentialsProvider crdsProvider; // 用户身份信息。
    private ResourceManager resourceManager;
    private AsyncServiceClient httpClient;
    private ScheduledExecutorService retryExecutor;
    private ExecutorService callbackExecutor; // 用于执行Callback
    private ClientConfiguration clientConfig;
    private RetryStrategy retryStrategy;
    private LauncherFactory launcherFactory;
    private Random random = new Random();
    private Cache<String, Long> timeseriesMetaCache;

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncClient}实例。
     *
     * @param endpoint
     *            TableStore服务的endpoint。
     * @param accessKeyId
     *            访问TableStore服务的Access ID。
     * @param accessKeySecret
     *            访问TableStore服务的Access Key。
     * @param instanceName
     *            访问TableStore服务的实例名称。
     */
    public InternalClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, null);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncClient}实例。
     *
     * @param endpoint
     *            TableStore服务的endpoint。
     * @param accessKeyId
     *            访问TableStore服务的Access ID。
     * @param accessKeySecret
     *            访问TableStore服务的Access Key。
     * @param instanceName
     *            访问TableStore服务的实例名称。
     * @param config
     *            客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     */
    public InternalClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName,
            ClientConfiguration config) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, null);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncClient}实例。
     *
     * @param endpoint
     *            TableStore服务的endpoint。
     * @param accessKeyId
     *            访问TableStore服务的Access ID。
     * @param accessKeySecret
     *            访问TableStore服务的Access Key。
     * @param instanceName
     *            访问TableStore服务的实例名称。
     * @param config
     *            客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param callbackExecutor
     *            用于执行用户在调用异步接口时传入的Callback。如果传入null则使用默认配置( 线程数与CPU核数相同的线程池)。
     */
    public InternalClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName,
            ClientConfiguration config, ExecutorService callbackExecutor) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, callbackExecutor, null);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncClient}实例。
     *
     * @param endpoint
     *            TableStore服务的endpoint。
     * @param accessKeyId
     *            访问TableStore服务的Access ID。
     * @param accessKeySecret
     *            访问TableStore服务的Access Key。
     * @param instanceName
     *            访问TableStore服务的实例名称。
     * @param config
     *            客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param callbackExecutor
     *            用于执行用户在调用异步接口时传入的Callback。如果传入null则使用默认配置( 线程数与CPU核数相同的线程池)。
     * @param stsToken
     *            Sts Token.
     */
    public InternalClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName,
                          ClientConfiguration config, ExecutorService callbackExecutor, String stsToken) {
        this(endpoint, CredentialsProviderFactory.newDefaultCredentialProvider(accessKeyId, accessKeySecret, stsToken),
                instanceName, config, new ResourceManager(config, callbackExecutor));
    }

    public InternalClient(String endpoint, CredentialsProvider credsProvider, String instanceName,
                          ClientConfiguration config, ResourceManager resourceManager) {
        Preconditions.checkArgument(endpoint != null && !endpoint.isEmpty(),
                "The end point should not be null or empty.");
        Preconditions.checkArgument(instanceName != null && !instanceName.isEmpty(),
                "The name of instance should not be null or empty.");
        Preconditions.checkArgument(instanceName.length() == instanceName.getBytes(Constants.UTF8_CHARSET).length,
                "InstanceName should not have multibyte character.");
        Preconditions.checkArgument(HttpUtil.checkSSRF(instanceName), "The instance name is invalid: " + instanceName);

        if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
            throw new IllegalArgumentException("the endpoint must start with \"http://\" or \"https://\".");
        }
        this.endpoint = endpoint;

        this.crdsProvider = credsProvider;

        if (config == null) {
            config = new ClientConfiguration();
        }

        this.clientConfig = config;

        this.retryStrategy = config.getRetryStrategy();

        this.instanceName = instanceName;

        if (resourceManager != null) {
            this.resourceManager = resourceManager;
        } else {
            this.resourceManager = new ResourceManager(this.clientConfig);
        }

        this.httpClient = this.resourceManager.getResources().getHttpClient();

        this.retryExecutor = this.resourceManager.getResources().getRetryExecutor();

        this.callbackExecutor = this.resourceManager.getResources().getCallbackExecutor();

        this.launcherFactory = new LauncherFactory(endpoint, instanceName, httpClient, crdsProvider, config);
    }

    public void setExtraHeaders(Map<String, String> extraHeaders) {
        this.httpClient.setExtraHeaders(extraHeaders);
    }

    /**
     * 返回访问的TableStore Endpoint。
     *
     * @return TableStore Endpoint。
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


    /**
     * 返回Client配置。
     * @return
     */
    public ClientConfiguration getClientConfig() {
        return clientConfig;
    }

    protected void setTimeseriesMetaCache(Cache<String, Long> cache) {
        timeseriesMetaCache = cache;
    }

    protected Cache<String, Long> getTimeseriesMetaCache() {
        return this.timeseriesMetaCache;
    }

    private TraceLogger getTraceLogger() {
        String traceId = new UUID(random.nextLong(), (new Random()).nextLong()).toString();
        return new TraceLogger(traceId, this.clientConfig.getTimeThresholdOfTraceLogger());
    }

    public Future<ListTableResponse> listTable(TableStoreCallback<ListTableRequest, ListTableResponse> callback) {
        ListTableRequest request = new ListTableRequest();
        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        ListTableLauncher launcher = launcherFactory.listTable(tracer, retry, request);

        AsyncCompletion<ListTableRequest, ListTableResponse> completion;
        completion = new AsyncCompletion<ListTableRequest, ListTableResponse>(launcher, request, tracer,
                callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<ListTableRequest, ListTableResponse> f = new CallbackImpledFuture<ListTableRequest, ListTableResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<CreateTableResponse> createTable(CreateTableRequest request,
            TableStoreCallback<CreateTableRequest, CreateTableResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        CreateTableLauncher launcher = launcherFactory.createTable(tracer, retry, request);

        AsyncCompletion<CreateTableRequest, CreateTableResponse> completion = new AsyncCompletion<CreateTableRequest, CreateTableResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<CreateTableRequest, CreateTableResponse> f = new CallbackImpledFuture<CreateTableRequest, CreateTableResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<CreateTableResponse> createTableEx(CreateTableRequestEx request,
                                                   TableStoreCallback<CreateTableRequestEx, CreateTableResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        CreateTableExLauncher launcher = launcherFactory.createTableEx(tracer, retry, request);

        AsyncCompletion<CreateTableRequestEx, CreateTableResponse> completion = new AsyncCompletion<CreateTableRequestEx, CreateTableResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<CreateTableRequestEx, CreateTableResponse> f = new CallbackImpledFuture<CreateTableRequestEx, CreateTableResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DescribeTableResponse> describeTable(DescribeTableRequest request,
            TableStoreCallback<DescribeTableRequest, DescribeTableResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DescribeTableLauncher launcher = launcherFactory.describeTable(tracer, retry, request);

        AsyncCompletion<DescribeTableRequest, DescribeTableResponse> completion;
        completion = new AsyncCompletion<DescribeTableRequest, DescribeTableResponse>(launcher, request, tracer,
                callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DescribeTableRequest, DescribeTableResponse> f = new CallbackImpledFuture<DescribeTableRequest, DescribeTableResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DeleteTableResponse> deleteTable(DeleteTableRequest request,
            TableStoreCallback<DeleteTableRequest, DeleteTableResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DeleteTableLauncher launcher = launcherFactory.deleteTable(tracer, retry, request);

        AsyncCompletion<DeleteTableRequest, DeleteTableResponse> completion;
        completion = new AsyncCompletion<DeleteTableRequest, DeleteTableResponse>(launcher, request, tracer,
                callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DeleteTableRequest, DeleteTableResponse> f = new CallbackImpledFuture<DeleteTableRequest, DeleteTableResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<UpdateTableResponse> updateTable(UpdateTableRequest request,
            TableStoreCallback<UpdateTableRequest, UpdateTableResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        UpdateTableLauncher launcher = launcherFactory.updateTable(tracer, retry, request);

        AsyncCompletion<UpdateTableRequest, UpdateTableResponse> completion;
        completion = new AsyncCompletion<UpdateTableRequest, UpdateTableResponse>(launcher, request, tracer,
                callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<UpdateTableRequest, UpdateTableResponse> f = new CallbackImpledFuture<UpdateTableRequest, UpdateTableResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<CreateIndexResponse> createIndex(CreateIndexRequest request,
                                                   TableStoreCallback<CreateIndexRequest, CreateIndexResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        CreateIndexLauncher launcher = launcherFactory.createIndex(tracer, retry, request);

        AsyncCompletion<CreateIndexRequest, CreateIndexResponse> completion = new AsyncCompletion<CreateIndexRequest, CreateIndexResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<CreateIndexRequest, CreateIndexResponse> f = new CallbackImpledFuture<CreateIndexRequest, CreateIndexResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DeleteIndexResponse> deleteIndex(DeleteIndexRequest request,
                                                   TableStoreCallback<DeleteIndexRequest, DeleteIndexResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DeleteIndexLauncher launcher = launcherFactory.deleteIndex(tracer, retry, request);

        AsyncCompletion<DeleteIndexRequest, DeleteIndexResponse> completion = new AsyncCompletion<DeleteIndexRequest, DeleteIndexResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DeleteIndexRequest, DeleteIndexResponse> f = new CallbackImpledFuture<DeleteIndexRequest, DeleteIndexResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<AddDefinedColumnResponse> addDefinedColumn(AddDefinedColumnRequest request,
                                                             TableStoreCallback<AddDefinedColumnRequest, AddDefinedColumnResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        AddDefinedColumnLauncher launcher = launcherFactory.addDefinedColumn(tracer, retry, request);

        AsyncCompletion<AddDefinedColumnRequest, AddDefinedColumnResponse> completion = new AsyncCompletion<AddDefinedColumnRequest, AddDefinedColumnResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<AddDefinedColumnRequest, AddDefinedColumnResponse> f = new CallbackImpledFuture<AddDefinedColumnRequest, AddDefinedColumnResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
        f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DeleteDefinedColumnResponse> deleteDefinedColumn(DeleteDefinedColumnRequest request,
                                                                   TableStoreCallback<DeleteDefinedColumnRequest, DeleteDefinedColumnResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DeleteDefinedColumnLauncher launcher = launcherFactory.deleteDefinedColumn(tracer, retry, request);
        AsyncCompletion<DeleteDefinedColumnRequest, DeleteDefinedColumnResponse> completion = new AsyncCompletion<DeleteDefinedColumnRequest, DeleteDefinedColumnResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DeleteDefinedColumnRequest, DeleteDefinedColumnResponse> f = new CallbackImpledFuture<DeleteDefinedColumnRequest, DeleteDefinedColumnResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<GetRowResponse> getRowInternal(GetRowRequest request,
            TableStoreCallback<GetRowRequest, GetRowResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        GetRowLauncher launcher = launcherFactory.getRow(tracer, retry, request);

        AsyncCompletion<GetRowRequest, GetRowResponse> completion = new AsyncCompletion<GetRowRequest, GetRowResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<GetRowRequest, GetRowResponse> f = new CallbackImpledFuture<GetRowRequest, GetRowResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<GetRowResponse> getRow(GetRowRequest request,
            TableStoreCallback<GetRowRequest, GetRowResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        GetRowLauncher launcher = launcherFactory.getRow(tracer, retry, request);

        AsyncGetRowCompletion completion = new AsyncGetRowCompletion(launcher, request, tracer, callbackExecutor, retry,
                retryExecutor);
        CallbackImpledFuture<GetRowRequest, GetRowResponse> f = new CallbackImpledFuture<GetRowRequest, GetRowResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<PutRowResponse> putRow(PutRowRequest request,
            TableStoreCallback<PutRowRequest, PutRowResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        PutRowLauncher launcher = launcherFactory.putRow(tracer, retry, request);

        AsyncCompletion<PutRowRequest, PutRowResponse> completion = new AsyncCompletion<PutRowRequest, PutRowResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<PutRowRequest, PutRowResponse> f = new CallbackImpledFuture<PutRowRequest, PutRowResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<UpdateRowResponse> updateRow(UpdateRowRequest request,
            TableStoreCallback<UpdateRowRequest, UpdateRowResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        UpdateRowLauncher launcher = launcherFactory.updateRow(tracer, retry, request);

        AsyncCompletion<UpdateRowRequest, UpdateRowResponse> completion = new AsyncCompletion<UpdateRowRequest, UpdateRowResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<UpdateRowRequest, UpdateRowResponse> f = new CallbackImpledFuture<UpdateRowRequest, UpdateRowResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DeleteRowResponse> deleteRow(DeleteRowRequest request,
            TableStoreCallback<DeleteRowRequest, DeleteRowResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DeleteRowLauncher launcher = launcherFactory.deleteRow(tracer, retry, request);

        AsyncCompletion<DeleteRowRequest, DeleteRowResponse> completion = new AsyncCompletion<DeleteRowRequest, DeleteRowResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DeleteRowRequest, DeleteRowResponse> f = new CallbackImpledFuture<DeleteRowRequest, DeleteRowResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<BatchGetRowResponse> batchGetRowInternal(BatchGetRowRequest request,
            TableStoreCallback<BatchGetRowRequest, BatchGetRowResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        BatchGetRowLauncher launcher = launcherFactory.batchGetRow(tracer, retry, request);

        AsyncCompletion<BatchGetRowRequest, BatchGetRowResponse> completion = new AsyncCompletion<BatchGetRowRequest, BatchGetRowResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<BatchGetRowRequest, BatchGetRowResponse> f = new CallbackImpledFuture<BatchGetRowRequest, BatchGetRowResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<BatchGetRowResponse> batchGetRow(BatchGetRowRequest request,
            TableStoreCallback<BatchGetRowRequest, BatchGetRowResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        BatchGetRowLauncher launcher = launcherFactory.batchGetRow(tracer, retry, request);

        AsyncBatchGetRowCompletion completion = new AsyncBatchGetRowCompletion(launcher, request, tracer,
                callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<BatchGetRowRequest, BatchGetRowResponse> f = new CallbackImpledFuture<BatchGetRowRequest, BatchGetRowResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<BatchWriteRowResponse> batchWriteRow(BatchWriteRowRequest request,
            TableStoreCallback<BatchWriteRowRequest, BatchWriteRowResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        BatchWriteRowLauncher launcher = launcherFactory.batchWriteRow(tracer, retry, request);

        AsyncCompletion<BatchWriteRowRequest, BatchWriteRowResponse> completion = new AsyncCompletion<BatchWriteRowRequest, BatchWriteRowResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<BatchWriteRowRequest, BatchWriteRowResponse> f = new CallbackImpledFuture<BatchWriteRowRequest, BatchWriteRowResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<BulkImportResponse> bulkImport(BulkImportRequest request,
                                                       TableStoreCallback<BulkImportRequest, BulkImportResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        BulkImportLauncher launcher = launcherFactory.bulkImport(tracer, retry, request);
        AsyncCompletion<BulkImportRequest, BulkImportResponse> completion = new AsyncCompletion<BulkImportRequest, BulkImportResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<BulkImportRequest, BulkImportResponse> f = new CallbackImpledFuture<BulkImportRequest, BulkImportResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<GetRangeResponse> getRangeInternal(GetRangeRequest request,
            TableStoreCallback<GetRangeRequest, GetRangeResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        GetRangeLauncher launcher = launcherFactory.getRange(tracer, retry, request);

        AsyncCompletion<GetRangeRequest, GetRangeResponse> completion = new AsyncCompletion<GetRangeRequest, GetRangeResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<GetRangeRequest, GetRangeResponse> f = new CallbackImpledFuture<GetRangeRequest, GetRangeResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<BulkExportResponse> bulkExportInternal(BulkExportRequest request,
                                                     TableStoreCallback<BulkExportRequest, BulkExportResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        BulkExportLauncher launcher = launcherFactory.bulkExport(tracer, retry, request);

        AsyncCompletion<BulkExportRequest, BulkExportResponse> completion = new AsyncCompletion<BulkExportRequest, BulkExportResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<BulkExportRequest, BulkExportResponse> f = new CallbackImpledFuture<BulkExportRequest, BulkExportResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<GetRangeResponse> getRange(GetRangeRequest request,
            TableStoreCallback<GetRangeRequest, GetRangeResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        GetRangeLauncher launcher = launcherFactory.getRange(tracer, retry, request);

        AsyncGetRangeCompletion completion = new AsyncGetRangeCompletion(launcher, request, tracer, callbackExecutor,
                retry, retryExecutor);
        CallbackImpledFuture<GetRangeRequest, GetRangeResponse> f = new CallbackImpledFuture<GetRangeRequest, GetRangeResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<BulkExportResponse> bulkExport(BulkExportRequest request,
                                             TableStoreCallback<BulkExportRequest, BulkExportResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        BulkExportLauncher launcher = launcherFactory.bulkExport(tracer, retry, request);

        AsyncCompletion<BulkExportRequest, BulkExportResponse> completion = new AsyncCompletion(launcher, request, tracer, callbackExecutor,
                retry, retryExecutor);
        CallbackImpledFuture<BulkExportRequest, BulkExportResponse> f = new CallbackImpledFuture<BulkExportRequest, BulkExportResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<ComputeSplitsBySizeResponse> computeSplitsBySize(ComputeSplitsBySizeRequest request,
            TableStoreCallback<ComputeSplitsBySizeRequest, ComputeSplitsBySizeResponse> callback) {
        Preconditions.checkNotNull(request);
        Preconditions.checkStringNotNullAndEmpty(request.getTableName(),
                "The table name for ComputeSplitsBySize should not be null or empty.");

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        ComputeSplitsBySizeLauncher launcher = launcherFactory.computeSplitsBySize(tracer, retry, request);

        AsyncCompletion<ComputeSplitsBySizeRequest, ComputeSplitsBySizeResponse> completion = new AsyncCompletion<ComputeSplitsBySizeRequest, ComputeSplitsBySizeResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<ComputeSplitsBySizeRequest, ComputeSplitsBySizeResponse> f = new CallbackImpledFuture<ComputeSplitsBySizeRequest, ComputeSplitsBySizeResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public void shutdown() {
        this.resourceManager.shutdown();
        if (this.timeseriesMetaCache != null) {
            this.timeseriesMetaCache.invalidateAll();
        }
    }

    public Future<ListStreamResponse> listStream(ListStreamRequest request,
            TableStoreCallback<ListStreamRequest, ListStreamResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        ListStreamLauncher launcher = launcherFactory.listStream(tracer, retry, request);

        AsyncCompletion<ListStreamRequest, ListStreamResponse> completion = new AsyncCompletion<ListStreamRequest, ListStreamResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<ListStreamRequest, ListStreamResponse> f = new CallbackImpledFuture<ListStreamRequest, ListStreamResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DescribeStreamResponse> describeStream(DescribeStreamRequest request,
            TableStoreCallback<DescribeStreamRequest, DescribeStreamResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DescribeStreamLauncher launcher = launcherFactory.describeStream(tracer, retry, request);

        AsyncCompletion<DescribeStreamRequest, DescribeStreamResponse> completion = new AsyncCompletion<DescribeStreamRequest, DescribeStreamResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DescribeStreamRequest, DescribeStreamResponse> f = new CallbackImpledFuture<DescribeStreamRequest, DescribeStreamResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<GetShardIteratorResponse> getShardIterator(GetShardIteratorRequest request,
            TableStoreCallback<GetShardIteratorRequest, GetShardIteratorResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        GetShardIteratorLauncher launcher = launcherFactory.getShardIterator(tracer, retry, request);

        AsyncCompletion<GetShardIteratorRequest, GetShardIteratorResponse> completion = new AsyncCompletion<GetShardIteratorRequest, GetShardIteratorResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<GetShardIteratorRequest, GetShardIteratorResponse> f = new CallbackImpledFuture<GetShardIteratorRequest, GetShardIteratorResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<GetStreamRecordResponse> getStreamRecord(GetStreamRecordRequest request,
            TableStoreCallback<GetStreamRecordRequest, GetStreamRecordResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        GetStreamRecordLauncher launcher = launcherFactory.getStreamRecord(tracer, retry, request);

        AsyncCompletion<GetStreamRecordRequest, GetStreamRecordResponse> completion = new AsyncCompletion<GetStreamRecordRequest, GetStreamRecordResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<GetStreamRecordRequest, GetStreamRecordResponse> f = new CallbackImpledFuture<GetStreamRecordRequest, GetStreamRecordResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<StartLocalTransactionResponse> startLocalTransaction(StartLocalTransactionRequest request,
            TableStoreCallback<StartLocalTransactionRequest, StartLocalTransactionResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        StartLocalTransactionLauncher launcher = launcherFactory.startLocalTransaction(tracer, retry, request);

        AsyncCompletion<StartLocalTransactionRequest, StartLocalTransactionResponse> completion = new AsyncCompletion<StartLocalTransactionRequest, StartLocalTransactionResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<StartLocalTransactionRequest, StartLocalTransactionResponse> f = new CallbackImpledFuture<StartLocalTransactionRequest, StartLocalTransactionResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<CommitTransactionResponse> commitTransaction(CommitTransactionRequest request,
            TableStoreCallback<CommitTransactionRequest, CommitTransactionResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        CommitTransactionLauncher launcher = launcherFactory.commitTransaction(tracer, retry, request);

        AsyncCompletion<CommitTransactionRequest, CommitTransactionResponse> completion = new AsyncCompletion<CommitTransactionRequest, CommitTransactionResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<CommitTransactionRequest, CommitTransactionResponse> f = new CallbackImpledFuture<CommitTransactionRequest, CommitTransactionResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<AbortTransactionResponse> abortTransaction(AbortTransactionRequest request,
            TableStoreCallback<AbortTransactionRequest, AbortTransactionResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        AbortTransactionLauncher launcher = launcherFactory.abortTransaction(tracer, retry, request);

        AsyncCompletion<AbortTransactionRequest, AbortTransactionResponse> completion = new AsyncCompletion<AbortTransactionRequest, AbortTransactionResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<AbortTransactionRequest, AbortTransactionResponse> f = new CallbackImpledFuture<AbortTransactionRequest, AbortTransactionResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<CreateSearchIndexResponse> createSearchIndex(CreateSearchIndexRequest request,
                                                               TableStoreCallback<CreateSearchIndexRequest, CreateSearchIndexResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        CreateSearchIndexLauncher launcher = launcherFactory.createSearchIndex(tracer, retry, request);

        AsyncCompletion<CreateSearchIndexRequest, CreateSearchIndexResponse> completion =
                new AsyncCompletion<CreateSearchIndexRequest, CreateSearchIndexResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<CreateSearchIndexRequest, CreateSearchIndexResponse> f =
                new CallbackImpledFuture<CreateSearchIndexRequest, CreateSearchIndexResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<UpdateSearchIndexResponse> updateSearchIndex(UpdateSearchIndexRequest request,
                                                               TableStoreCallback<UpdateSearchIndexRequest, UpdateSearchIndexResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        UpdateSearchIndexLauncher launcher = launcherFactory.updateSearchIndex(tracer, retry, request);

        AsyncCompletion<UpdateSearchIndexRequest, UpdateSearchIndexResponse> completion =
                new AsyncCompletion<UpdateSearchIndexRequest, UpdateSearchIndexResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<UpdateSearchIndexRequest, UpdateSearchIndexResponse> f =
                new CallbackImpledFuture<UpdateSearchIndexRequest, UpdateSearchIndexResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<ListSearchIndexResponse> listSearchIndex(ListSearchIndexRequest request,
                                                           TableStoreCallback<ListSearchIndexRequest, ListSearchIndexResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        ListSearchIndexLauncher launcher = launcherFactory.listSearchIndex(tracer, retry, request);

        AsyncCompletion<ListSearchIndexRequest, ListSearchIndexResponse> completion =
                new AsyncCompletion<ListSearchIndexRequest, ListSearchIndexResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<ListSearchIndexRequest, ListSearchIndexResponse> f =
                new CallbackImpledFuture<ListSearchIndexRequest, ListSearchIndexResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DeleteSearchIndexResponse> deleteSearchIndex(DeleteSearchIndexRequest request,
                                                               TableStoreCallback<DeleteSearchIndexRequest, DeleteSearchIndexResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DeleteSearchIndexLauncher launcher = launcherFactory.deleteSearchIndex(tracer, retry, request);

        AsyncCompletion<DeleteSearchIndexRequest, DeleteSearchIndexResponse> completion =
                new AsyncCompletion<DeleteSearchIndexRequest, DeleteSearchIndexResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DeleteSearchIndexRequest, DeleteSearchIndexResponse> f =
                new CallbackImpledFuture<DeleteSearchIndexRequest, DeleteSearchIndexResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DescribeSearchIndexResponse> describeSearchIndex(DescribeSearchIndexRequest request,
                                                               TableStoreCallback<DescribeSearchIndexRequest, DescribeSearchIndexResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DescribeSearchIndexLauncher launcher = launcherFactory.describeSearchIndex(tracer, retry, request);

        AsyncCompletion<DescribeSearchIndexRequest, DescribeSearchIndexResponse> completion =
                new AsyncCompletion<DescribeSearchIndexRequest, DescribeSearchIndexResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DescribeSearchIndexRequest, DescribeSearchIndexResponse> f =
                new CallbackImpledFuture<DescribeSearchIndexRequest, DescribeSearchIndexResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<ComputeSplitsResponse> computeSplits(ComputeSplitsRequest request, TableStoreCallback<ComputeSplitsRequest, ComputeSplitsResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        ComputeSplitsLauncher launcher = launcherFactory.computeSplits(tracer, retry, request);

        AsyncCompletion<ComputeSplitsRequest, ComputeSplitsResponse> completion =
            new AsyncCompletion<ComputeSplitsRequest, ComputeSplitsResponse>(launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<ComputeSplitsRequest, ComputeSplitsResponse> f = new CallbackImpledFuture<ComputeSplitsRequest, ComputeSplitsResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<ParallelScanResponse> parallelScan(ParallelScanRequest request, TableStoreCallback<ParallelScanRequest, ParallelScanResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        ParallelScanLauncher launcher = launcherFactory.parallelScan(tracer, retry, request);

        AsyncCompletion<ParallelScanRequest, ParallelScanResponse> completion =
            new AsyncCompletion<ParallelScanRequest, ParallelScanResponse>(launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<ParallelScanRequest, ParallelScanResponse> f = new CallbackImpledFuture<ParallelScanRequest, ParallelScanResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<SearchResponse> search(SearchRequest request, TableStoreCallback<SearchRequest, SearchResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        SearchLauncher launcher = launcherFactory.search(tracer, retry, request);

        AsyncCompletion<SearchRequest, SearchResponse> completion =
                new AsyncCompletion<SearchRequest, SearchResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<SearchRequest, SearchResponse> f =
                new CallbackImpledFuture<SearchRequest, SearchResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<CreateTunnelResponse> createTunnel(CreateTunnelRequest request,
                                                     TableStoreCallback<CreateTunnelRequest, CreateTunnelResponse> callback) {
        Preconditions.checkNotNull(request);
        TraceLogger tracer = getTraceLogger();

        RetryStrategy retry = this.retryStrategy.clone();
        CreateTunnelLauncher launcher = launcherFactory.createTunnel(tracer, retry, request);

        AsyncCompletion<CreateTunnelRequest, CreateTunnelResponse> completion =
            new AsyncCompletion<CreateTunnelRequest, CreateTunnelResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor
            );
        CallbackImpledFuture<CreateTunnelRequest, CreateTunnelResponse> f =
            new CallbackImpledFuture<CreateTunnelRequest, CreateTunnelResponse>();
        completion.watchBy(f);

        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<ListTunnelResponse> listTunnel(ListTunnelRequest request,
                                                 TableStoreCallback<ListTunnelRequest, ListTunnelResponse> callback) {
        Preconditions.checkNotNull(request);
        TraceLogger tracer = getTraceLogger();

        RetryStrategy retry = this.retryStrategy.clone();
        ListTunnelLauncher launcher = launcherFactory.listTunnel(tracer, retry, request);

        AsyncCompletion<ListTunnelRequest, ListTunnelResponse> completion =
            new AsyncCompletion<ListTunnelRequest, ListTunnelResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor
            );
        CallbackImpledFuture<ListTunnelRequest, ListTunnelResponse> f =
            new CallbackImpledFuture<ListTunnelRequest, ListTunnelResponse>();
        completion.watchBy(f);

        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DescribeTunnelResponse> describeTunnel(DescribeTunnelRequest request,
                                                         TableStoreCallback<DescribeTunnelRequest, DescribeTunnelResponse> callback) {
        Preconditions.checkNotNull(request);
        TraceLogger tracer = getTraceLogger();

        RetryStrategy retry = this.retryStrategy.clone();
        DescribeTunnelLauncher launcher = launcherFactory.describeTunnel(tracer, retry, request);

        AsyncCompletion<DescribeTunnelRequest, DescribeTunnelResponse> completion =
            new AsyncCompletion<DescribeTunnelRequest, DescribeTunnelResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor
            );
        CallbackImpledFuture<DescribeTunnelRequest, DescribeTunnelResponse> f =
            new CallbackImpledFuture<DescribeTunnelRequest, DescribeTunnelResponse>();
        completion.watchBy(f);

        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DeleteTunnelResponse> deleteTunnel(DeleteTunnelRequest request,
                                                     TableStoreCallback<DeleteTunnelRequest, DeleteTunnelResponse> callback) {
        Preconditions.checkNotNull(request);
        TraceLogger tracer = getTraceLogger();

        RetryStrategy retry = this.retryStrategy.clone();
        DeleteTunnelLauncher launcher = launcherFactory.deleteTunnel(tracer, retry, request);

        AsyncCompletion<DeleteTunnelRequest, DeleteTunnelResponse> completion =
            new AsyncCompletion<DeleteTunnelRequest, DeleteTunnelResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor
            );
        CallbackImpledFuture<DeleteTunnelRequest, DeleteTunnelResponse> f =
            new CallbackImpledFuture<DeleteTunnelRequest, DeleteTunnelResponse>();
        completion.watchBy(f);

        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<ConnectTunnelResponse> connectTunnel(ConnectTunnelRequest request,
                                                       TableStoreCallback<ConnectTunnelRequest, ConnectTunnelResponse> callback) {
        Preconditions.checkNotNull(request);
        TraceLogger tracer = getTraceLogger();

        RetryStrategy retry = this.retryStrategy.clone();
        ConnectTunnelLauncher launcher = launcherFactory.connectTunnel(tracer, retry, request);

        AsyncCompletion<ConnectTunnelRequest, ConnectTunnelResponse> completion =
            new AsyncCompletion<ConnectTunnelRequest, ConnectTunnelResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor
            );
        CallbackImpledFuture<ConnectTunnelRequest, ConnectTunnelResponse> f =
            new CallbackImpledFuture<ConnectTunnelRequest, ConnectTunnelResponse>();
        completion.watchBy(f);

        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }


    public Future<HeartbeatResponse> heartbeat(HeartbeatRequest request,
                                                     TableStoreCallback<HeartbeatRequest, HeartbeatResponse> callback) {
        Preconditions.checkNotNull(request);
        TraceLogger tracer = getTraceLogger();

        RetryStrategy retry = this.retryStrategy.clone();
        HeartbeatLauncher launcher = launcherFactory.heartbeat(tracer, retry, request);

        AsyncCompletion<HeartbeatRequest, HeartbeatResponse> completion =
            new AsyncCompletion<HeartbeatRequest, HeartbeatResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor
            );
        CallbackImpledFuture<HeartbeatRequest, HeartbeatResponse> f =
            new CallbackImpledFuture<HeartbeatRequest, HeartbeatResponse>();
        completion.watchBy(f);

        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<ShutdownTunnelResponse> shutdownTunnel(ShutdownTunnelRequest request,
                                                     TableStoreCallback<ShutdownTunnelRequest, ShutdownTunnelResponse> callback) {
        Preconditions.checkNotNull(request);
        TraceLogger tracer = getTraceLogger();

        RetryStrategy retry = this.retryStrategy.clone();
        ShutdownTunnelLauncher launcher = launcherFactory.shutdownTunnel(tracer, retry, request);

        AsyncCompletion<ShutdownTunnelRequest, ShutdownTunnelResponse> completion =
            new AsyncCompletion<ShutdownTunnelRequest, ShutdownTunnelResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor
            );
        CallbackImpledFuture<ShutdownTunnelRequest, ShutdownTunnelResponse> f =
            new CallbackImpledFuture<ShutdownTunnelRequest, ShutdownTunnelResponse>();
        completion.watchBy(f);

        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<GetCheckpointResponse> getCheckpoint(GetCheckpointRequest request,
                                                       TableStoreCallback<GetCheckpointRequest, GetCheckpointResponse> callback) {
        Preconditions.checkNotNull(request);
        TraceLogger tracer = getTraceLogger();

        RetryStrategy retry = this.retryStrategy.clone();
        GetCheckpointLauncher launcher = launcherFactory.getCheckpoint(tracer, retry, request);

        AsyncCompletion<GetCheckpointRequest, GetCheckpointResponse> completion =
            new AsyncCompletion<GetCheckpointRequest, GetCheckpointResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor
            );
        CallbackImpledFuture<GetCheckpointRequest, GetCheckpointResponse> f =
            new CallbackImpledFuture<GetCheckpointRequest, GetCheckpointResponse>();
        completion.watchBy(f);

        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<ReadRecordsResponse> readRecords(ReadRecordsRequest request,
                                                   TableStoreCallback<ReadRecordsRequest, ReadRecordsResponse> callback) {
        Preconditions.checkNotNull(request);
        TraceLogger tracer = getTraceLogger();

        RetryStrategy retry = this.retryStrategy.clone();
        ReadRecordsLauncher launcher = launcherFactory.readRecords(tracer, retry, request);

        AsyncCompletion<ReadRecordsRequest, ReadRecordsResponse> completion =
            new AsyncCompletion<ReadRecordsRequest, ReadRecordsResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor
            );
        CallbackImpledFuture<ReadRecordsRequest, ReadRecordsResponse> f =
            new CallbackImpledFuture<ReadRecordsRequest, ReadRecordsResponse>();
        completion.watchBy(f);

        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<CheckpointResponse> checkpoint(CheckpointRequest request,
                                                 TableStoreCallback<CheckpointRequest, CheckpointResponse> callback) {
        Preconditions.checkNotNull(request);
        TraceLogger tracer = getTraceLogger();

        RetryStrategy retry = this.retryStrategy.clone();
        CheckpointLauncher launcher = launcherFactory.checkpoint(tracer, retry, request);

        AsyncCompletion<CheckpointRequest, CheckpointResponse> completion =
            new AsyncCompletion<CheckpointRequest, CheckpointResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor
            );
        CallbackImpledFuture<CheckpointRequest, CheckpointResponse> f =
            new CallbackImpledFuture<CheckpointRequest, CheckpointResponse>();
        completion.watchBy(f);

        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }


    public Future<CreateDeliveryTaskResponse> createDeliveryTask(CreateDeliveryTaskRequest request,
                                                                 TableStoreCallback<CreateDeliveryTaskRequest, CreateDeliveryTaskResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        CreateDeliveryTaskLauncher launcher = launcherFactory.createDeliveryTask(tracer, retry, request);

        AsyncCompletion<CreateDeliveryTaskRequest, CreateDeliveryTaskResponse> completion = new AsyncCompletion<CreateDeliveryTaskRequest, CreateDeliveryTaskResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<CreateDeliveryTaskRequest, CreateDeliveryTaskResponse> f = new CallbackImpledFuture<CreateDeliveryTaskRequest, CreateDeliveryTaskResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DeleteDeliveryTaskResponse> deleteDeliveryTask(DeleteDeliveryTaskRequest request,
                                                                 TableStoreCallback<DeleteDeliveryTaskRequest, DeleteDeliveryTaskResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DeleteDeliveryTaskLauncher launcher = launcherFactory.deleteDeliveryTask(tracer, retry, request);

        AsyncCompletion<DeleteDeliveryTaskRequest, DeleteDeliveryTaskResponse> completion = new AsyncCompletion<DeleteDeliveryTaskRequest, DeleteDeliveryTaskResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DeleteDeliveryTaskRequest, DeleteDeliveryTaskResponse> f = new CallbackImpledFuture<DeleteDeliveryTaskRequest, DeleteDeliveryTaskResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DescribeDeliveryTaskResponse> describeDeliveryTask(DescribeDeliveryTaskRequest request,
                                                                     TableStoreCallback<DescribeDeliveryTaskRequest, DescribeDeliveryTaskResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DescribeDeliveryTaskLauncher launcher = launcherFactory.describeDeliveryTask(tracer, retry, request);

        AsyncCompletion<DescribeDeliveryTaskRequest, DescribeDeliveryTaskResponse> completion = new AsyncCompletion<DescribeDeliveryTaskRequest, DescribeDeliveryTaskResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DescribeDeliveryTaskRequest, DescribeDeliveryTaskResponse> f = new CallbackImpledFuture<DescribeDeliveryTaskRequest, DescribeDeliveryTaskResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<ListDeliveryTaskResponse> listDeliveryTask(ListDeliveryTaskRequest request,
                                                             TableStoreCallback<ListDeliveryTaskRequest, ListDeliveryTaskResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        ListDeliveryTaskLauncher launcher = launcherFactory.listDeliveryTask(tracer, retry, request);

        AsyncCompletion<ListDeliveryTaskRequest, ListDeliveryTaskResponse> completion = new AsyncCompletion<ListDeliveryTaskRequest, ListDeliveryTaskResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<ListDeliveryTaskRequest, ListDeliveryTaskResponse> f = new CallbackImpledFuture<ListDeliveryTaskRequest, ListDeliveryTaskResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<CreateTimeseriesTableResponse> createTimeseriesTable(CreateTimeseriesTableRequest request,
                                                               TableStoreCallback<CreateTimeseriesTableRequest, CreateTimeseriesTableResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        CreateTimeseriesTableLauncher launcher = launcherFactory.createTimeseriesTable(tracer, retry, request);

        AsyncCompletion<CreateTimeseriesTableRequest, CreateTimeseriesTableResponse> completion =
                new AsyncCompletion<CreateTimeseriesTableRequest, CreateTimeseriesTableResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<CreateTimeseriesTableRequest, CreateTimeseriesTableResponse> f =
                new CallbackImpledFuture<CreateTimeseriesTableRequest, CreateTimeseriesTableResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DeleteTimeseriesTableResponse> deleteTimeseriesTable(DeleteTimeseriesTableRequest request,
                                                                       TableStoreCallback<DeleteTimeseriesTableRequest, DeleteTimeseriesTableResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DeleteTimeseriesTableLauncher launcher = launcherFactory.deleteTimeseriesTable(tracer, retry, request);

        AsyncCompletion<DeleteTimeseriesTableRequest, DeleteTimeseriesTableResponse> completion =
                new AsyncCompletion<DeleteTimeseriesTableRequest, DeleteTimeseriesTableResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DeleteTimeseriesTableRequest, DeleteTimeseriesTableResponse> f =
                new CallbackImpledFuture<DeleteTimeseriesTableRequest, DeleteTimeseriesTableResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DescribeTimeseriesTableResponse> describeTimeseriesTable(DescribeTimeseriesTableRequest request,
                                                                       TableStoreCallback<DescribeTimeseriesTableRequest, DescribeTimeseriesTableResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DescribeTimeseriesTableLauncher launcher = launcherFactory.describeTimeseriesTable(tracer, retry, request);

        AsyncCompletion<DescribeTimeseriesTableRequest, DescribeTimeseriesTableResponse> completion =
                new AsyncCompletion<DescribeTimeseriesTableRequest, DescribeTimeseriesTableResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DescribeTimeseriesTableRequest, DescribeTimeseriesTableResponse> f =
                new CallbackImpledFuture<DescribeTimeseriesTableRequest, DescribeTimeseriesTableResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<UpdateTimeseriesTableResponse> updateTimeseriesTable(UpdateTimeseriesTableRequest request,
                                                                           TableStoreCallback<UpdateTimeseriesTableRequest, UpdateTimeseriesTableResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        UpdateTimeseriesTableLauncher launcher = launcherFactory.updateTimeseriesTable(tracer, retry, request);

        AsyncCompletion<UpdateTimeseriesTableRequest, UpdateTimeseriesTableResponse> completion =
                new AsyncCompletion<UpdateTimeseriesTableRequest, UpdateTimeseriesTableResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<UpdateTimeseriesTableRequest, UpdateTimeseriesTableResponse> f =
                new CallbackImpledFuture<UpdateTimeseriesTableRequest, UpdateTimeseriesTableResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<PutTimeseriesDataResponse> putTimeseriesData(PutTimeseriesDataRequest request,
                                                               TableStoreCallback<PutTimeseriesDataRequest, PutTimeseriesDataResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        PutTimeseriesDataLauncher launcher = launcherFactory.putTimeseriesData(tracer, retry, request, timeseriesMetaCache);

        AsyncCompletion<PutTimeseriesDataRequest, PutTimeseriesDataResponse> completion =
                new AsyncCompletion<PutTimeseriesDataRequest, PutTimeseriesDataResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<PutTimeseriesDataRequest, PutTimeseriesDataResponse> f =
                new CallbackImpledFuture<PutTimeseriesDataRequest, PutTimeseriesDataResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<GetTimeseriesDataResponse> getTimeseriesData(GetTimeseriesDataRequest request,
                                                               TableStoreCallback<GetTimeseriesDataRequest, GetTimeseriesDataResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        GetTimeseriesDataLauncher launcher = launcherFactory.getTimeseriesData(tracer, retry, request);

        AsyncCompletion<GetTimeseriesDataRequest, GetTimeseriesDataResponse> completion =
            new AsyncCompletion<GetTimeseriesDataRequest, GetTimeseriesDataResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<GetTimeseriesDataRequest, GetTimeseriesDataResponse> f =
            new CallbackImpledFuture<GetTimeseriesDataRequest, GetTimeseriesDataResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<ListTimeseriesTableResponse> listTimeseriesTable(TableStoreCallback<ListTimeseriesTableRequest, ListTimeseriesTableResponse> callback) {
        ListTimeseriesTableRequest request = new ListTimeseriesTableRequest();
        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        ListTimeseriesTableLauncher launcher = launcherFactory.listTimeseriesTable(tracer, retry, request);

        AsyncCompletion<ListTimeseriesTableRequest, ListTimeseriesTableResponse> completion =
                new AsyncCompletion<ListTimeseriesTableRequest, ListTimeseriesTableResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<ListTimeseriesTableRequest, ListTimeseriesTableResponse> f =
                new CallbackImpledFuture<ListTimeseriesTableRequest, ListTimeseriesTableResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<QueryTimeseriesMetaResponse> queryTimeseriesMeta(QueryTimeseriesMetaRequest request,
                                                                   TableStoreCallback<QueryTimeseriesMetaRequest, QueryTimeseriesMetaResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        QueryTimeseriesMetaLauncher launcher = launcherFactory.queryTimeseriesMeta(tracer, retry, request);

        AsyncCompletion<QueryTimeseriesMetaRequest, QueryTimeseriesMetaResponse> completion =
            new AsyncCompletion<QueryTimeseriesMetaRequest, QueryTimeseriesMetaResponse>(
                launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<QueryTimeseriesMetaRequest, QueryTimeseriesMetaResponse> f =
            new CallbackImpledFuture<QueryTimeseriesMetaRequest, QueryTimeseriesMetaResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<UpdateTimeseriesMetaResponse> updateTimeseriesMeta(UpdateTimeseriesMetaRequest request,
                                                                     TableStoreCallback<UpdateTimeseriesMetaRequest, UpdateTimeseriesMetaResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        UpdateTimeseriesMetaLauncher launcher = launcherFactory.updateTimeseriesMeta(tracer, retry, request);

        AsyncCompletion<UpdateTimeseriesMetaRequest, UpdateTimeseriesMetaResponse> completion =
                new AsyncCompletion<UpdateTimeseriesMetaRequest, UpdateTimeseriesMetaResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<UpdateTimeseriesMetaRequest, UpdateTimeseriesMetaResponse> f =
                new CallbackImpledFuture<UpdateTimeseriesMetaRequest, UpdateTimeseriesMetaResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DeleteTimeseriesMetaResponse> deleteTimeseriesMeta(DeleteTimeseriesMetaRequest request,
                                                                     TableStoreCallback<DeleteTimeseriesMetaRequest, DeleteTimeseriesMetaResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DeleteTimeseriesMetaLauncher launcher = launcherFactory.deleteTimeseriesMeta(tracer, retry, request);

        AsyncCompletion<DeleteTimeseriesMetaRequest, DeleteTimeseriesMetaResponse> completion =
                new AsyncCompletion<DeleteTimeseriesMetaRequest, DeleteTimeseriesMetaResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DeleteTimeseriesMetaRequest, DeleteTimeseriesMetaResponse> f =
                new CallbackImpledFuture<DeleteTimeseriesMetaRequest, DeleteTimeseriesMetaResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<SplitTimeseriesScanTaskResponse> splitTimeseriesScanTask(
            SplitTimeseriesScanTaskRequest request,
            TableStoreCallback<SplitTimeseriesScanTaskRequest, SplitTimeseriesScanTaskResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        SplitTimeseriesScanTaskLauncher launcher = launcherFactory.splitTimeseriesScanTask(tracer, retry, request);

        AsyncCompletion<SplitTimeseriesScanTaskRequest, SplitTimeseriesScanTaskResponse> completion =
                new AsyncCompletion<SplitTimeseriesScanTaskRequest, SplitTimeseriesScanTaskResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<SplitTimeseriesScanTaskRequest, SplitTimeseriesScanTaskResponse> f =
                new CallbackImpledFuture<SplitTimeseriesScanTaskRequest, SplitTimeseriesScanTaskResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<ScanTimeseriesDataResponse> scanTimeseriesData(
            ScanTimeseriesDataRequest request,
            TableStoreCallback<ScanTimeseriesDataRequest, ScanTimeseriesDataResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        ScanTimeseriesDataLauncher launcher = launcherFactory.scanTimeseriesData(tracer, retry, request);

        AsyncCompletion<ScanTimeseriesDataRequest, ScanTimeseriesDataResponse> completion =
                new AsyncCompletion<ScanTimeseriesDataRequest, ScanTimeseriesDataResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<ScanTimeseriesDataRequest, ScanTimeseriesDataResponse> f =
                new CallbackImpledFuture<ScanTimeseriesDataRequest, ScanTimeseriesDataResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<CreateTimeseriesAnalyticalStoreResponse> createTimeseriesAnalyticalStore(
            CreateTimeseriesAnalyticalStoreRequest request,
            TableStoreCallback<CreateTimeseriesAnalyticalStoreRequest, CreateTimeseriesAnalyticalStoreResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        CreateTimeseriesAnalyticalStoreLauncher launcher = launcherFactory.createTimeseriesAnalyticalStore(tracer, retry, request);

        AsyncCompletion<CreateTimeseriesAnalyticalStoreRequest, CreateTimeseriesAnalyticalStoreResponse> completion =
                new AsyncCompletion<CreateTimeseriesAnalyticalStoreRequest, CreateTimeseriesAnalyticalStoreResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<CreateTimeseriesAnalyticalStoreRequest, CreateTimeseriesAnalyticalStoreResponse> f =
                new CallbackImpledFuture<CreateTimeseriesAnalyticalStoreRequest, CreateTimeseriesAnalyticalStoreResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DeleteTimeseriesAnalyticalStoreResponse> deleteTimeseriesAnalyticalStore(
            DeleteTimeseriesAnalyticalStoreRequest request,
            TableStoreCallback<DeleteTimeseriesAnalyticalStoreRequest, DeleteTimeseriesAnalyticalStoreResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DeleteTimeseriesAnalyticalStoreLauncher launcher = launcherFactory.deleteTimeseriesAnalyticalStore(tracer, retry, request);

        AsyncCompletion<DeleteTimeseriesAnalyticalStoreRequest, DeleteTimeseriesAnalyticalStoreResponse> completion =
                new AsyncCompletion<DeleteTimeseriesAnalyticalStoreRequest, DeleteTimeseriesAnalyticalStoreResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DeleteTimeseriesAnalyticalStoreRequest, DeleteTimeseriesAnalyticalStoreResponse> f =
                new CallbackImpledFuture<DeleteTimeseriesAnalyticalStoreRequest, DeleteTimeseriesAnalyticalStoreResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<DescribeTimeseriesAnalyticalStoreResponse> describeTimeseriesAnalyticalStore(
            DescribeTimeseriesAnalyticalStoreRequest request,
            TableStoreCallback<DescribeTimeseriesAnalyticalStoreRequest, DescribeTimeseriesAnalyticalStoreResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        DescribeTimeseriesAnalyticalStoreLauncher launcher = launcherFactory.describeTimeseriesAnalyticalStore(tracer, retry, request);

        AsyncCompletion<DescribeTimeseriesAnalyticalStoreRequest, DescribeTimeseriesAnalyticalStoreResponse> completion =
                new AsyncCompletion<DescribeTimeseriesAnalyticalStoreRequest, DescribeTimeseriesAnalyticalStoreResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<DescribeTimeseriesAnalyticalStoreRequest, DescribeTimeseriesAnalyticalStoreResponse> f =
                new CallbackImpledFuture<DescribeTimeseriesAnalyticalStoreRequest, DescribeTimeseriesAnalyticalStoreResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public Future<UpdateTimeseriesAnalyticalStoreResponse> updateTimeseriesAnalyticalStore(
            UpdateTimeseriesAnalyticalStoreRequest request,
            TableStoreCallback<UpdateTimeseriesAnalyticalStoreRequest, UpdateTimeseriesAnalyticalStoreResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        UpdateTimeseriesAnalyticalStoreLauncher launcher = launcherFactory.updateTimeseriesAnalyticalStore(tracer, retry, request);

        AsyncCompletion<UpdateTimeseriesAnalyticalStoreRequest, UpdateTimeseriesAnalyticalStoreResponse> completion =
                new AsyncCompletion<UpdateTimeseriesAnalyticalStoreRequest, UpdateTimeseriesAnalyticalStoreResponse>(
                        launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<UpdateTimeseriesAnalyticalStoreRequest, UpdateTimeseriesAnalyticalStoreResponse> f =
                new CallbackImpledFuture<UpdateTimeseriesAnalyticalStoreRequest, UpdateTimeseriesAnalyticalStoreResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }

    public void setCredentials(ServiceCredentials credentials) {
        CredentialsProvider newCrdsProvider = CredentialsProviderFactory.newDefaultCredentialProvider(credentials.getAccessKeyId(),
                credentials.getAccessKeySecret(), credentials.getSecurityToken());
        switchCredentialsProvider(newCrdsProvider);
    }

    public void switchCredentialsProvider(CredentialsProvider newCrdsProvider) {
        this.crdsProvider = newCrdsProvider;
        this.launcherFactory.setCredentialsProvider(newCrdsProvider);
    }

    public Future<SQLQueryResponse> sqlQuery(SQLQueryRequest request,
                                             TableStoreCallback<SQLQueryRequest, SQLQueryResponse> callback) {
        Preconditions.checkNotNull(request);

        TraceLogger tracer = getTraceLogger();
        RetryStrategy retry = this.retryStrategy.clone();
        SQLQueryLauncher launcher = launcherFactory.sqlQuery(tracer, retry, request);

        AsyncCompletion<SQLQueryRequest, SQLQueryResponse> completion =
                                new AsyncCompletion<SQLQueryRequest, SQLQueryResponse>(launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        CallbackImpledFuture<SQLQueryRequest, SQLQueryResponse> f = new CallbackImpledFuture<SQLQueryRequest, SQLQueryResponse>();
        completion.watchBy(f);
        if (callback != null) {
            // user callback must be triggered after completion of the return
            // future.
            f.watchBy(callback);
        }

        launcher.fire(request, completion);

        return f;
    }
}
