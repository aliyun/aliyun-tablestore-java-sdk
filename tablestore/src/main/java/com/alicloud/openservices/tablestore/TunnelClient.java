package com.alicloud.openservices.tablestore;

import java.util.Map;
import java.util.concurrent.*;

import com.alicloud.openservices.tablestore.core.ResourceManager;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
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
     * Constructs a new {@link TunnelClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     */
    public TunnelClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName) {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setEnableResponseValidation(false);
        this.internalClient = new InternalClient(endpoint, accessKeyId, accessKeySecret, instanceName, conf);
    }

    /**
     * Constructs a new {@link TunnelClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
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
     * Constructs a new {@link TunnelClient} instance using the specified TableStore Endpoint and configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param config          The client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     */
    public TunnelClient(String endpoint, String accessKeyId,
                        String accessKeySecret, String instanceName,
                        ClientConfiguration config) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, null);
    }

    /**
     * Constructs a new {@link TunnelClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param config          The client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     * @param stsToken        Sts Token.
     */
    public TunnelClient(String endpoint, String accessKeyId,
                        String accessKeySecret, String instanceName, ClientConfiguration config, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, stsToken, null);
    }

    /**
     * Constructs a new {@link TunnelClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint         The endpoint of the TableStore service.
     * @param accessKeyId      The Access ID for accessing the TableStore service.
     * @param accessKeySecret  The Access Key for accessing the TableStore service.
     * @param instanceName     The instance name for accessing the TableStore service.
     * @param config           The client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     * @param stsToken         Sts Token.
     * @param callbackExecutor The thread pool for executing callbacks. Note that this thread pool will also be shut down when the client is shut down.
     */
    public TunnelClient(String endpoint, String accessKeyId,
                        String accessKeySecret, String instanceName, ClientConfiguration config, String stsToken,
                        ExecutorService callbackExecutor) {
        if (config == null) {
            config = new ClientConfiguration();
        }
        config.setEnableResponseValidation(false);

        this.internalClient = new InternalClient(endpoint, accessKeyId, accessKeySecret, instanceName, config,
            callbackExecutor, stsToken);
    }

    /**
     * Constructs a new {@link TunnelClient} instance using the specified TableStore Endpoint and default configurations.
     */
    public TunnelClient(String endpoint, CredentialsProvider credsProvider, String instanceName,
                            ClientConfiguration config, ResourceManager resourceManager) {
        if (config == null) {
            config = new ClientConfiguration();
        }
        config.setEnableResponseValidation(false);

        this.internalClient = new InternalClient(endpoint, credsProvider, instanceName, config, resourceManager);
    }

    TunnelClient(InternalClient internalClient) {
        this.internalClient = internalClient;
    }

    public void setExtraHeaders(Map<String, String> extraHeaders) {
        this.internalClient.setExtraHeaders(extraHeaders);
    }

    /**
     * Returns the TableStore Endpoint being accessed.
     *
     * @return TableStore Endpoint.
     */
    public String getEndpoint() {
        return this.internalClient.getEndpoint();
    }

    /**
     * Returns the name of the accessed instance
     *
     * @return instance name
     */
    public String getInstanceName() {
        return this.internalClient.getInstanceName();
    }

    /**
     * Returns the client configuration
     *
     * @return client configuration
     */
    public ClientConfiguration getClientConfig() {
        return this.internalClient.getClientConfig();
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
            return f.get(this.internalClient.getClientConfig().getSyncClientWaitFutureTimeoutInMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new ClientException(String.format(
                "The thread was interrupted: %s", e.getMessage()));
        } catch (ExecutionException e) {
            throw new ClientException("The thread was aborted", e);
        } catch (TimeoutException e) {
            throw new ClientException("Wait future timeout", e);
        }
    }

    @Override
    public void shutdown() {
        this.internalClient.shutdown();
    }
}
