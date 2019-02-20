package com.alicloud.openservices.tablestore.tunnel.unittest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TunnelClientInterface;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RecordColumn;
import com.alicloud.openservices.tablestore.model.StreamRecord;
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
import com.google.gson.Gson;

import static com.alicloud.openservices.tablestore.core.protocol.ResponseFactory.FINISH_TAG;

public class MockTunnelClient implements TunnelClientInterface {
    @Override
    public CreateTunnelResponse createTunnel(CreateTunnelRequest request) throws TableStoreException, ClientException {
        return null;
    }

    @Override
    public ListTunnelResponse listTunnel(ListTunnelRequest request) throws TableStoreException, ClientException {
        return null;
    }

    @Override
    public DescribeTunnelResponse describeTunnel(DescribeTunnelRequest request)
        throws TableStoreException, ClientException {
        return null;
    }

    @Override
    public DeleteTunnelResponse deleteTunnel(DeleteTunnelRequest request) throws TableStoreException, ClientException {
        return null;
    }

    @Override
    public ConnectTunnelResponse connectTunnel(ConnectTunnelRequest request)
        throws TableStoreException, ClientException {
        return null;
    }

    @Override
    public HeartbeatResponse heartbeat(HeartbeatRequest request) throws TableStoreException, ClientException {
        return null;
    }

    @Override
    public ShutdownTunnelResponse shutdownTunnel(ShutdownTunnelRequest request)
        throws TableStoreException, ClientException {
        return null;
    }

    @Override
    public GetCheckpointResponse getCheckpoint(GetCheckpointRequest request)
        throws TableStoreException, ClientException {
        if (request.getChannelId().contains("getCheckpointFailed")) {
            throw new TableStoreException("get checkpoint failed", ErrorCode.INVALID_PARAMETER);
        }
        GetCheckpointResponse resp = new GetCheckpointResponse();
        resp.setCheckpoint("token");
        resp.setSequenceNumber(1);
        return resp;
    }

    public static volatile List<String> finishedChannels = new ArrayList<String>();
    @Override
    public ReadRecordsResponse readRecords(ReadRecordsRequest request) throws TableStoreException, ClientException {
        ReadRecordsResponse resp = new ReadRecordsResponse();
        List<StreamRecord> recordList = new ArrayList<StreamRecord>();
        for (int i = 0; i < 100; i++) {
            StreamRecord record = new StreamRecord();
            record.setRecordType(StreamRecord.RecordType.PUT);
            record.setPrimaryKey(new PrimaryKey(Arrays.asList(new PrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i)))));
            record.setColumns(Arrays.asList(new RecordColumn(new Column("col", ColumnValue.fromLong(i), i), RecordColumn.ColumnType.PUT)));
            recordList.add(record);
        }
        resp.setRecords(recordList);
        if (finishedChannels.contains(request.getChannelId())) {
            resp.setNextToken(FINISH_TAG);
        } else {
            resp.setNextToken("token");
        }
        resp.setMemoizedSerializedSize(102400);
        return resp;
    }

    @Override
    public CheckpointResponse checkpoint(CheckpointRequest request) throws TableStoreException, ClientException {
        return null;
    }

    @Override
    public void shutdown() {

    }
}
