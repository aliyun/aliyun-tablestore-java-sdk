package com.alicloud.openservices.tablestore;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
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

public class TunnelClient implements TunnelClientInterface {
    private InternalClient internalClient;

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link TunnelClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     */
    public TunnelClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName) {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setEnableResponseValidation(false);
        this.internalClient = new InternalClient(endpoint, accessKeyId, accessKeySecret, instanceName, conf);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link TunnelClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     * @param stsToken        Sts Token.
     */
    public TunnelClient(String endpoint, String accessKeyId,
                        String accessKeySecret, String instanceName, String stsToken) {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setEnableResponseValidation(false);
        this.internalClient = new InternalClient(endpoint, accessKeyId, accessKeySecret, instanceName, conf, null,
            stsToken);
    }

    /**
     * 使用指定的TableStore Endpoint和配置构造一个新的{@link TunnelClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     * @param config          客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     */
    public TunnelClient(String endpoint, String accessKeyId,
                        String accessKeySecret, String instanceName,
                        ClientConfiguration config) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, null);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link TunnelClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     * @param config          客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param stsToken        Sts Token.
     */
    public TunnelClient(String endpoint, String accessKeyId,
                        String accessKeySecret, String instanceName, ClientConfiguration config, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, stsToken, null);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link TunnelClient}实例。
     *
     * @param endpoint         TableStore服务的endpoint。
     * @param accessKeyId      访问TableStore服务的Access ID。
     * @param accessKeySecret  访问TableStore服务的Access Key。
     * @param instanceName     访问TableStore服务的实例名称。
     * @param config           客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param stsToken         Sts Token.
     * @param callbackExecutor 执行callback的线程池，需要注意的是，client在shutdown的时候也会shutdown这个线程池。
     */
    public TunnelClient(String endpoint, String accessKeyId,
                        String accessKeySecret, String instanceName, ClientConfiguration config, String stsToken,
                        ExecutorService callbackExecutor) {
        if (config != null) {
            config.setEnableResponseValidation(false);
        } else {
            config = new ClientConfiguration();
            config.setEnableResponseValidation(false);
        }
        this.internalClient = new InternalClient(endpoint, accessKeyId, accessKeySecret, instanceName, config,
            callbackExecutor, stsToken);
    }

    TunnelClient(InternalClient internalClient) {
        this.internalClient = internalClient;
    }

    public void setExtraHeaders(Map<String, String> extraHeaders) {
        this.internalClient.setExtraHeaders(extraHeaders);
    }

    /**
     * 返回访问的TableStore Endpoint。
     *
     * @return TableStore Endpoint。
     */
    public String getEndpoint() {
        return this.internalClient.getEndpoint();
    }

    /**
     * 返回访问的实例的名称
     *
     * @return instance name
     */
    public String getInstanceName() {
        return this.internalClient.getInstanceName();
    }

    @Override
    public CreateTunnelResponse createTunnel(CreateTunnelRequest request)
        throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Future<CreateTunnelResponse> resp = internalClient.createTunnel(request, null);
        return waitForFuture(resp);
    }

    @Override
    public ListTunnelResponse listTunnel(ListTunnelRequest request)
        throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Future<ListTunnelResponse> resp = internalClient.listTunnel(request, null);
        return waitForFuture(resp);
    }

    @Override
    public DescribeTunnelResponse describeTunnel(DescribeTunnelRequest request)
        throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Future<DescribeTunnelResponse> resp = internalClient.describeTunnel(request, null);
        return waitForFuture(resp);
    }

    @Override
    public DeleteTunnelResponse deleteTunnel(DeleteTunnelRequest request)
        throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Future<DeleteTunnelResponse> resp = internalClient.deleteTunnel(request, null);
        return waitForFuture(resp);
    }

    @Override
    public ConnectTunnelResponse connectTunnel(ConnectTunnelRequest request)
        throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Future<ConnectTunnelResponse> resp = internalClient.connectTunnel(request, null);
        return waitForFuture(resp);
    }

    @Override
    public HeartbeatResponse heartbeat(HeartbeatRequest request)
        throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Future<HeartbeatResponse> resp = internalClient.heartbeat(request, null);
        return waitForFuture(resp);
    }

    @Override
    public ShutdownTunnelResponse shutdownTunnel(ShutdownTunnelRequest request)
        throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Future<ShutdownTunnelResponse> resp = internalClient.shutdownTunnel(request, null);
        return waitForFuture(resp);
    }

    @Override
    public GetCheckpointResponse getCheckpoint(GetCheckpointRequest request)
        throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Future<GetCheckpointResponse> resp = internalClient.getCheckpoint(request, null);
        return waitForFuture(resp);
    }

    @Override
    public ReadRecordsResponse readRecords(ReadRecordsRequest request)
        throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Future<ReadRecordsResponse> resp = internalClient.readRecords(request, null);
        return waitForFuture(resp);
    }

    @Override
    public CheckpointResponse checkpoint(CheckpointRequest request)
        throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Future<CheckpointResponse> resp = internalClient.checkpoint(request, null);
        return waitForFuture(resp);
    }

    private <Res> Res waitForFuture(Future<Res> f) {
        try {
            return f.get();
        } catch (InterruptedException e) {
            throw new ClientException(String.format(
                "The thread was interrupted: %s", e.getMessage()));
        } catch (ExecutionException e) {
            throw new ClientException("The thread was aborted", e);
        }
    }

    @Override
    public void shutdown() {
        this.internalClient.shutdown();
    }
}
