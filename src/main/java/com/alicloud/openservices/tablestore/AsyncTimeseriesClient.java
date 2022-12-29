package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.core.ResourceManager;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentialProvider;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import java.util.Map;
import java.util.concurrent.*;

public class AsyncTimeseriesClient implements AsyncTimeseriesClientInterface {

    private InternalClient internalClient;
    private Cache<String, Long> timeseriesMetaCache;

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncTimeseriesClient}实例。
     * @param endpoint        TableStore服务的Endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore的服务的实例名称。
     */
    public AsyncTimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, new ClientConfiguration());
    }

    /**
     * 使用指定的TableStore Endpoint和配置构造一个新的{@link AsyncTimeseriesClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     * @param conf            客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     */
    public AsyncTimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName, ClientConfiguration conf) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, conf, null);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncTimeseriesClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     * @param stsToken        Sts Token.
     */
    public AsyncTimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, new ClientConfiguration(), stsToken);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncTimeseriesClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     * @param config          客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param stsToken        Sts Token.
     */
    public AsyncTimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName,
                            ClientConfiguration config, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, stsToken, null);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncTimeseriesClient}实例。
     *
     * @param endpoint         TableStore服务的endpoint。
     * @param accessKeyId      访问TableStore服务的Access ID。
     * @param accessKeySecret  访问TableStore服务的Access Key。
     * @param instanceName     访问TableStore服务的实例名称。
     * @param config           客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param stsToken         Sts Token.
     * @param callbackExecutor 执行callback的线程池，需要注意的是，client在shutdown的时候也会shutdown这个线程池。
     */
    public AsyncTimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName,
                            ClientConfiguration config, String stsToken, ExecutorService callbackExecutor) {
        this(endpoint, new DefaultCredentialProvider(accessKeyId, accessKeySecret, stsToken), instanceName,
                config, new ResourceManager(config, callbackExecutor));
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncTimeseriesClient}实例。
     */
    public AsyncTimeseriesClient(String endpoint, CredentialsProvider credsProvider, String instanceName,
                                 ClientConfiguration config, ResourceManager resourceManager) {
        this(new InternalClient(endpoint, credsProvider, instanceName, config, resourceManager));
    }

    AsyncTimeseriesClient(InternalClient internalClient) {
        Preconditions.checkNotNull(internalClient);
        this.internalClient = internalClient;
        if (this.internalClient.getTimeseriesMetaCache() != null) {
            this.timeseriesMetaCache = this.internalClient.getTimeseriesMetaCache();
            return;
        }
        ClientConfiguration config = this.internalClient.getClientConfig();
        if (config.getTimeseriesConfiguration() == null) {
            config.setTimeseriesConfiguration(new TimeseriesConfiguration());
        }
        this.timeseriesMetaCache = CacheBuilder.newBuilder()
                .maximumWeight(config.getTimeseriesConfiguration().getMetaCacheMaxDataSize())
                .expireAfterAccess(config.getConnectionTimeoutInMillisecond(), TimeUnit.SECONDS)
                .weigher(new Weigher<String, Long>() {
                    @Override
                    public int weigh(String key, Long value) {
                        return key.length() + 48 + 16; // add some overhead
                    }
                }).build();
        this.internalClient.setTimeseriesMetaCache(timeseriesMetaCache);
    }

    public void setExtraHeaders(Map<String, String> extraHeaders) {
        this.internalClient.setExtraHeaders(extraHeaders);
    }

    @Override
    public Future<CreateTimeseriesTableResponse> createTimeseriesTable(CreateTimeseriesTableRequest request,
                                                                       TableStoreCallback<CreateTimeseriesTableRequest, CreateTimeseriesTableResponse> callback)
    {
        return internalClient.createTimeseriesTable(request, callback);
    }

    @Override
    public Future<ListTimeseriesTableResponse> listTimeseriesTable(TableStoreCallback<ListTimeseriesTableRequest, ListTimeseriesTableResponse> callback)
    {
        return internalClient.listTimeseriesTable(callback);
    }

    @Override
    public Future<DeleteTimeseriesTableResponse> deleteTimeseriesTable(DeleteTimeseriesTableRequest request,
                                                                       TableStoreCallback<DeleteTimeseriesTableRequest, DeleteTimeseriesTableResponse> callback)
    {
        return internalClient.deleteTimeseriesTable(request, callback);
    }

    @Override
    public Future<DescribeTimeseriesTableResponse> describeTimeseriesTable(DescribeTimeseriesTableRequest request,
                                                                           TableStoreCallback<DescribeTimeseriesTableRequest, DescribeTimeseriesTableResponse> callback)
    {
        return internalClient.describeTimeseriesTable(request, callback);
    }

    @Override
    public Future<UpdateTimeseriesTableResponse> updateTimeseriesTable(UpdateTimeseriesTableRequest request,
                                                                       TableStoreCallback<UpdateTimeseriesTableRequest, UpdateTimeseriesTableResponse> callback)
    {
        return internalClient.updateTimeseriesTable(request, callback);
    }

    @Override
    public Future<PutTimeseriesDataResponse> putTimeseriesData(PutTimeseriesDataRequest request,
                                                               TableStoreCallback<PutTimeseriesDataRequest, PutTimeseriesDataResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.putTimeseriesData(request, callback);
    }

    @Override
    public Future<GetTimeseriesDataResponse> getTimeseriesData(GetTimeseriesDataRequest request,
                                                               TableStoreCallback<GetTimeseriesDataRequest, GetTimeseriesDataResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.getTimeseriesData(request, callback);
    }

    @Override
    public Future<QueryTimeseriesMetaResponse> queryTimeseriesMeta(QueryTimeseriesMetaRequest request,
                                                                   TableStoreCallback<QueryTimeseriesMetaRequest, QueryTimeseriesMetaResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.queryTimeseriesMeta(request, callback);
    }

    @Override
    public Future<UpdateTimeseriesMetaResponse> updateTimeseriesMeta(UpdateTimeseriesMetaRequest request,
                                                                     TableStoreCallback<UpdateTimeseriesMetaRequest, UpdateTimeseriesMetaResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.updateTimeseriesMeta(request, callback);
    }

    @Override
    public Future<DeleteTimeseriesMetaResponse> deleteTimeseriesMeta(DeleteTimeseriesMetaRequest request,
                                                                     TableStoreCallback<DeleteTimeseriesMetaRequest, DeleteTimeseriesMetaResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.deleteTimeseriesMeta(request, callback);
    }

    @Override
    public Future<SplitTimeseriesScanTaskResponse> splitTimeseriesScanTask(SplitTimeseriesScanTaskRequest request,
                                                                           TableStoreCallback<SplitTimeseriesScanTaskRequest, SplitTimeseriesScanTaskResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.splitTimeseriesScanTask(request, callback);
    }

    @Override
    public Future<ScanTimeseriesDataResponse> scanTimeseriesData(ScanTimeseriesDataRequest request,
                                                                 TableStoreCallback<ScanTimeseriesDataRequest, ScanTimeseriesDataResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.scanTimeseriesData(request, callback);
    }

    public void shutdown() {
        this.internalClient.shutdown();
    }

    public SyncClient asSyncClient() {
        return new SyncClient(this.internalClient);
    }

    public AsyncClient asAsyncClient() {
        return new AsyncClient(this.internalClient);
    }

    public TimeseriesClient asTimeseriesClient() {
        return new TimeseriesClient(this.internalClient);
    }
}
