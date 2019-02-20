package com.alicloud.openservices.tablestore.core.protocol;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.model.tunnel.ChannelStatus;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.ListTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelType;
import com.alicloud.openservices.tablestore.model.tunnel.internal.Channel;
import com.alicloud.openservices.tablestore.model.tunnel.internal.CheckpointRequest;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelClientConfig;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ConnectTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.HeartbeatRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ShutdownTunnelRequest;

public class TunnelProtocolBuilder {
    public static TunnelServiceApi.TunnelType buildTunnelType(TunnelType tunnelType) {
        switch (tunnelType) {
            case BaseData:
                return TunnelServiceApi.TunnelType.BaseData;
            case Stream:
                return TunnelServiceApi.TunnelType.Stream;
            case BaseAndStream:
                return TunnelServiceApi.TunnelType.BaseAndStream;
            default:
                throw new IllegalArgumentException("unknown tunnelType: " + tunnelType.name());
        }
    }

    public static TunnelServiceApi.Tunnel buildTunnel(String tableName, String tunnelName, TunnelType tunnelType) {
        TunnelServiceApi.Tunnel.Builder builder = TunnelServiceApi.Tunnel.newBuilder();
        builder.setTableName(tableName);
        builder.setTunnelName(tunnelName);
        builder.setTunnelType(buildTunnelType(tunnelType));
        return builder.build();
    }

    public static TunnelServiceApi.CreateTunnelRequest buildCreateTunnelRequest(CreateTunnelRequest request) {
        TunnelServiceApi.CreateTunnelRequest.Builder builder = TunnelServiceApi.CreateTunnelRequest.newBuilder();
        builder.setTunnel(buildTunnel(request.getTableName(), request.getTunnelName(), request.getTunnelType()));
        return builder.build();
    }

    public static TunnelServiceApi.ListTunnelRequest buildListTunnelRequest(ListTunnelRequest request) {
        TunnelServiceApi.ListTunnelRequest.Builder builder = TunnelServiceApi.ListTunnelRequest.newBuilder();
        builder.setTableName(request.getTableName());
        return builder.build();
    }

    public static TunnelServiceApi.DescribeTunnelRequest buildDescribeTunnelRequest(DescribeTunnelRequest request) {
        TunnelServiceApi.DescribeTunnelRequest.Builder builder = TunnelServiceApi.DescribeTunnelRequest.newBuilder();
        builder.setTableName(request.getTableName());
        builder.setTunnelName(request.getTunnelName());
        return builder.build();
    }

    public static TunnelServiceApi.DeleteTunnelRequest buildDeleteTunnelRequest(DeleteTunnelRequest request) {
        TunnelServiceApi.DeleteTunnelRequest.Builder builder = TunnelServiceApi.DeleteTunnelRequest.newBuilder();
        builder.setTableName(request.getTableName());
        builder.setTunnelName(request.getTunnelName());
        return builder.build();
    }

    public static TunnelServiceApi.ClientConfig buildClientConfig(TunnelClientConfig config) {
        TunnelServiceApi.ClientConfig.Builder builder = TunnelServiceApi.ClientConfig.newBuilder();
        builder.setTimeout(config.getTimeout());
        builder.setClientTag(config.getClientTag());
        return builder.build();
    }

    public static TunnelServiceApi.ConnectRequest buildConnectTunnelRequest(ConnectTunnelRequest request) {
        TunnelServiceApi.ConnectRequest.Builder builder = TunnelServiceApi.ConnectRequest.newBuilder();
        builder.setTunnelId(request.getTunnelId());
        builder.setClientConfig(buildClientConfig(request.getConfig()));
        return builder.build();
    }

    public static TunnelServiceApi.ChannelStatus buildChannelStatus(ChannelStatus status) {
        switch (status) {
            case OPEN:
                return TunnelServiceApi.ChannelStatus.OPEN;
            case CLOSING:
                return TunnelServiceApi.ChannelStatus.CLOSING;
            case CLOSE:
                return TunnelServiceApi.ChannelStatus.CLOSE;
            case TERMINATED:
                return TunnelServiceApi.ChannelStatus.TERMINATED;
            default:
                throw new IllegalArgumentException("unknown channel status: " + status.name());
        }
    }

    public static TunnelServiceApi.Channel buildChannel(Channel channel) {
        TunnelServiceApi.Channel.Builder builder = TunnelServiceApi.Channel.newBuilder();
        builder.setChannelId(channel.getChannelId());
        builder.setVersion(channel.getVersion());
        builder.setStatus(buildChannelStatus(channel.getStatus()));
        return builder.build();
    }

    public static List<TunnelServiceApi.Channel> buildChannels(List<Channel> channels) {
        List<TunnelServiceApi.Channel> retList = new ArrayList<TunnelServiceApi.Channel>();
        for (Channel channel : channels) {
            retList.add(buildChannel(channel));
        }
        return retList;
    }

    public static TunnelServiceApi.HeartbeatRequest buildHeartbeatRequest(HeartbeatRequest request) {
        TunnelServiceApi.HeartbeatRequest.Builder builder = TunnelServiceApi.HeartbeatRequest.newBuilder();
        builder.setTunnelId(request.getTunnelId());
        builder.setClientId(request.getClientId());
        builder.addAllChannels(buildChannels(request.getChannels()));
        return builder.build();
    }

    public static TunnelServiceApi.ShutdownRequest buildShutdownTunnelRequest(ShutdownTunnelRequest request) {
        TunnelServiceApi.ShutdownRequest.Builder builder = TunnelServiceApi.ShutdownRequest.newBuilder();
        builder.setTunnelId(request.getTunnelId());
        builder.setClientId(request.getClientId());
        return builder.build();
    }

    public static TunnelServiceApi.GetCheckpointRequest buildGetCheckpointRequest(GetCheckpointRequest request) {
       TunnelServiceApi.GetCheckpointRequest.Builder builder = TunnelServiceApi.GetCheckpointRequest.newBuilder();
       builder.setTunnelId(request.getTunnelId());
       builder.setClientId(request.getClientId());
       builder.setChannelId(request.getChannelId());
       return builder.build();
    }

    public static TunnelServiceApi.ReadRecordsRequest buildReadRecordsRequest(ReadRecordsRequest request) {
        TunnelServiceApi.ReadRecordsRequest.Builder builder = TunnelServiceApi.ReadRecordsRequest.newBuilder();
        builder.setTunnelId(request.getTunneId());
        builder.setClientId(request.getClientId());
        builder.setChannelId(request.getChannelId());
        builder.setToken(request.getToken());
        return builder.build();
    }

    public static TunnelServiceApi.CheckpointRequest buildCheckpointRequest(CheckpointRequest request) {
       TunnelServiceApi.CheckpointRequest.Builder builder = TunnelServiceApi.CheckpointRequest.newBuilder();
       builder.setTunnelId(request.getTunnelId());
       builder.setClientId(request.getClientId());
       builder.setChannelId(request.getChannelId());
       builder.setCheckpoint(request.getCheckpoint());
       builder.setSequenceNumber(request.getSequenceNumber());
       return builder.build();
    }
}
