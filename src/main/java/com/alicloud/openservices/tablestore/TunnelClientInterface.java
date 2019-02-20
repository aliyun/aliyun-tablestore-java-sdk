package com.alicloud.openservices.tablestore;

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

public interface TunnelClientInterface {
    /**
     * 创建一个Tunnel。
     * @param request  创建Tunnel所需的参数，详见{@link CreateTunnelRequest}
     * @return 创建Tunnel返回的结果, 详见{@link CreateTunnelResponse}
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    CreateTunnelResponse createTunnel(CreateTunnelRequest request)
        throws TableStoreException, ClientException;

    /**
     * 获取某个表下的Tunnel信息。
     * @param request 列举某张表下的Tunnel所需的参数，详见{@link ListTunnelRequest}
     * @return Tunnel的详细信息列表, 详见{@link ListTunnelResponse}
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    ListTunnelResponse listTunnel(ListTunnelRequest request)
        throws TableStoreException, ClientException;

    /**
     * 获取某个Tunnel下的具体信息。
     * @param request 描述某个Tunnel下的详细信息所需的参数，详见{@link DescribeTunnelRequest}
     * @return Tunnel下的具体信息，包括Channel信息和RPO信息等，详见{@link DescribeTunnelResponse}
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    DescribeTunnelResponse describeTunnel(DescribeTunnelRequest request)
        throws TableStoreException, ClientException;

    /**
     * 删除一个Tunnel。
     * @param request 删除某个Tunnel所需的参数，详见{@link DeleteTunnelRequest}
     * @return 删除Tunnel的结果，详见{@link DeleteTunnelResponse}
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    DeleteTunnelResponse deleteTunnel(DeleteTunnelRequest request)
        throws TableStoreException, ClientException;

    /**
     * 高级接口: 不推荐直接使用，无特殊需求，请优先使用TunnelWorker自动化数据处理框架。
     * 为指定Tunnel分配客户端标识(ClientId), 同时可以传输一些Client的参数给Master，比如client心跳超时时间，client的类型等。
     * @param request 连接某个Tunnel所需的参数，详见{@link ConnectTunnelRequest}
     * @return 连接Tunnel的结果，详见{@link ConnectTunnelResponse}
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    ConnectTunnelResponse connectTunnel(ConnectTunnelRequest request)
        throws TableStoreException, ClientException;

    /**
     * 高级接口: 不推荐直接使用，无特殊需求，请优先使用TunnelWorker自动化数据处理框架。
     * Heartbeat操作的作用为(TunnelClient)保持心跳，同时可以获取可以消费的Channel，上报一些处理状态。
     *      当心跳超时后认为TunnelClient下线，需要重新Connect。
     * @param request 探测心跳所需的参数，详见{@link HeartbeatRequest}
     * @return 探测心跳的结果，详见{@link HeartbeatResponse}
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    HeartbeatResponse heartbeat(HeartbeatRequest request)
        throws TableStoreException, ClientException;

    /**
     * 高级接口: 不推荐直接使用，无特殊需求，请优先使用TunnelWorker自动化数据处理框架。
     * 关闭某个TunnelClient, 断开与Tunnel的服务端的连接。
     * @param request 关闭TunnelClient所需的参数，详见{@link ShutdownTunnelRequest}
     * @return 关闭TunnelClient的结果，详见{@link ShutdownTunnelResponse}
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    ShutdownTunnelResponse shutdownTunnel(ShutdownTunnelRequest request)
        throws TableStoreException, ClientException;

    /**
     * 高级接口: 不推荐直接使用，无特殊需求，请优先使用TunnelWorker自动化数据处理框架。
     * 获取某个Channel上次记录的Checkpoint和Checkpoint对应的SequenceNumber。
     * @param request 获取某个Channel的Checkpoint所需的参数，详见{@link GetCheckpointRequest}
     * @return 某个Channel上次记录的Checkpoint结果
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    GetCheckpointResponse getCheckpoint(GetCheckpointRequest request)
        throws TableStoreException, ClientException;

    /**
     * 高级接口: 不推荐直接使用，无特殊需求，请优先使用TunnelWorker自动化数据处理框架。
     * 从某个Channel中读取数据，读取时需要指定Tunnel ID， Client ID, Channel ID和起始Token值。第一次读取时，
     *    使用上次记录的Token(Checkpoint)开始读取，之后每次使用返回的NextToken开始读取。
     * @param request 读取某个Channel中的数据， 详见{@link ReadRecordsRequest}
     * @return 某个Channel上的数据(全量或增量类型数据)
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    ReadRecordsResponse readRecords(ReadRecordsRequest request)
        throws TableStoreException, ClientException;

    /**
     * 高级接口: 不推荐直接使用，无特殊需求，请优先使用TunnelWorker自动化数据处理框架。
     * 设置某个Channel的Checkpoint和Checkpoint对应的SequenceNumber。
     * @param request 设置某个Channel上的Checkpoint
     * @return
     * @throws TableStoreException TableStore服务返回的异常
     * @throws ClientException 请求的返回结果无效、或遇到网络异常
     */
    CheckpointResponse checkpoint(CheckpointRequest request)
        throws TableStoreException, ClientException;

    /**
     * 释放资源。
     * <p>请确保在所有请求执行完毕之后释放资源。释放资源之后将不能再发送请求，正在执行的请求可能无法返回结果。</p>
     */
    void shutdown();
}
