package com.alicloud.openservices.tablestore.core;

import java.util.Map;
import java.util.HashMap;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.ListTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.CheckpointRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ConnectTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.HeartbeatRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ShutdownTunnelRequest;

import static com.alicloud.openservices.tablestore.model.OperationNames.*;

public class LauncherFactory {
    private Map<String, Context> contexts = new HashMap<String, Context>();
    private String instanceName;
    private AsyncServiceClient client;
    private CredentialsProvider crdsProvider;
    private ClientConfiguration config;

    private class Context {
        public OTSUri uri;

        public Context(OTSUri uri) {
            this.uri = uri;
        }
    }

    public void setCredentialsProvider(CredentialsProvider crdsProvider) {
        this.crdsProvider = crdsProvider;
    }

    public LauncherFactory(
        String endpoint,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config)
    {
        this.instanceName = instanceName;
        this.client = client;
        this.crdsProvider = crdsProvider;
        this.config = config;

        contexts.put(OP_CREATE_TABLE,
            new Context(new OTSUri(endpoint, OP_CREATE_TABLE)));
        contexts.put(OP_DELETE_TABLE,
            new Context(new OTSUri(endpoint, OP_DELETE_TABLE)));
        contexts.put(OP_DESCRIBE_TABLE,
            new Context(new OTSUri(endpoint, OP_DESCRIBE_TABLE)));
        contexts.put(OP_LIST_TABLE,
            new Context(new OTSUri(endpoint, OP_LIST_TABLE)));
        contexts.put(OP_UPDATE_TABLE,
            new Context(new OTSUri(endpoint, OP_UPDATE_TABLE)));
        contexts.put(OP_CREATE_INDEX,
            new Context(new OTSUri(endpoint, OP_CREATE_INDEX)));
        contexts.put(OP_DELETE_INDEX,
            new Context(new OTSUri(endpoint, OP_DELETE_INDEX)));

        contexts.put(OP_PUT_ROW,
            new Context(new OTSUri(endpoint, OP_PUT_ROW)));
        contexts.put(OP_UPDATE_ROW,
            new Context(new OTSUri(endpoint, OP_UPDATE_ROW)));
        contexts.put(OP_DELETE_ROW,
            new Context(new OTSUri(endpoint, OP_DELETE_ROW)));
        contexts.put(OP_GET_ROW,
                new Context(new OTSUri(endpoint, OP_GET_ROW)));
        contexts.put(OP_BATCH_GET_ROW,
                new Context(new OTSUri(endpoint, OP_BATCH_GET_ROW)));
        contexts.put(OP_BATCH_WRITE_ROW,
                new Context(new OTSUri(endpoint, OP_BATCH_WRITE_ROW)));
        contexts.put(OP_GET_RANGE,
                new Context(new OTSUri(endpoint, OP_GET_RANGE)));
        contexts.put(OP_LIST_STREAM,
                new Context(new OTSUri(endpoint, OP_LIST_STREAM)));
        contexts.put(OP_DESCRIBE_STREAM,
                new Context(new OTSUri(endpoint, OP_DESCRIBE_STREAM)));
        contexts.put(OP_GET_SHARD_ITERATOR,
                new Context(new OTSUri(endpoint, OP_GET_SHARD_ITERATOR)));
        contexts.put(OP_GET_STREAM_RECORD,
                new Context(new OTSUri(endpoint, OP_GET_STREAM_RECORD)));
        contexts.put(OP_COMPUTE_SPLITS_BY_SIZE,
                new Context(new OTSUri(endpoint, OP_COMPUTE_SPLITS_BY_SIZE)));
        contexts.put(OP_CREATE_SEARCH_INDEX,
                new Context(new OTSUri(endpoint, OP_CREATE_SEARCH_INDEX)));
        contexts.put(OP_LIST_SEARCH_INDEX,
                new Context(new OTSUri(endpoint, OP_LIST_SEARCH_INDEX)));
        contexts.put(OP_DELETE_SEARCH_INDEX,
                new Context(new OTSUri(endpoint, OP_DELETE_SEARCH_INDEX)));
        contexts.put(OP_DESCRIBE_SEARCH_INDEX,
                new Context(new OTSUri(endpoint, OP_DESCRIBE_SEARCH_INDEX)));
        contexts.put(OP_SEARCH,
                new Context(new OTSUri(endpoint, OP_SEARCH)));
        contexts.put(OP_START_LOCAL_TRANSACTION,
                new Context(new OTSUri(endpoint, OP_START_LOCAL_TRANSACTION)));
        contexts.put(OP_COMMIT_TRANSACTION,
                new Context(new OTSUri(endpoint, OP_COMMIT_TRANSACTION)));
        contexts.put(OP_ABORT_TRANSACTION,
                new Context(new OTSUri(endpoint, OP_ABORT_TRANSACTION)));

        contexts.put(OP_CREATE_TUNNEL,
            new Context(new OTSUri(endpoint, OP_CREATE_TUNNEL)));
        contexts.put(OP_LIST_TUNNEL,
            new Context(new OTSUri(endpoint, OP_LIST_TUNNEL)));
        contexts.put(OP_DESCRIBE_TUNNEL,
            new Context(new OTSUri(endpoint, OP_DESCRIBE_TUNNEL)));
        contexts.put(OP_DELETE_TUNNEL,
            new Context(new OTSUri(endpoint, OP_DELETE_TUNNEL)));
        contexts.put(OP_CONNECT_TUNNEL,
            new Context(new OTSUri(endpoint, OP_CONNECT_TUNNEL)));
        contexts.put(OP_HEARTBEAT,
            new Context(new OTSUri(endpoint, OP_HEARTBEAT)));
        contexts.put(OP_SHUTDOWN_TUNNEL,
            new Context(new OTSUri(endpoint, OP_SHUTDOWN_TUNNEL)));
        contexts.put(OP_GETCHECKPOINT,
            new Context(new OTSUri(endpoint, OP_GETCHECKPOINT)));
        contexts.put(OP_READRECORDS,
            new Context(new OTSUri(endpoint, OP_READRECORDS)));
        contexts.put(OP_CHECKPOINT,
            new Context(new OTSUri(endpoint, OP_CHECKPOINT)));
    }

    public CreateTableLauncher createTable(TraceLogger tracer, RetryStrategy retry, CreateTableRequest originRequest)
    {
        Context ctx = contexts.get(OP_CREATE_TABLE);
        return new CreateTableLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public DeleteTableLauncher deleteTable(TraceLogger tracer, RetryStrategy retry, DeleteTableRequest originRequest)
    {
        Context ctx = contexts.get(OP_DELETE_TABLE);
        return new DeleteTableLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public DescribeTableLauncher describeTable(TraceLogger tracer, RetryStrategy retry, DescribeTableRequest originRequest)
    {
        Context ctx = contexts.get(OP_DESCRIBE_TABLE);
        return new DescribeTableLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public ListTableLauncher listTable(TraceLogger tracer, RetryStrategy retry, ListTableRequest originRequest)
    {
        Context ctx = contexts.get(OP_LIST_TABLE);
        return new ListTableLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public UpdateTableLauncher updateTable(TraceLogger tracer, RetryStrategy retry, UpdateTableRequest originRequest)
    {
        Context ctx = contexts.get(OP_UPDATE_TABLE);
        return new UpdateTableLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public CreateIndexLauncher createIndex(TraceLogger tracer, RetryStrategy retry, CreateIndexRequest originRequest)
    {
        Context ctx = contexts.get(OP_CREATE_INDEX);
        return new CreateIndexLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public DeleteIndexLauncher deleteIndex(TraceLogger tracer, RetryStrategy retry, DeleteIndexRequest originRequest)
    {
        Context ctx = contexts.get(OP_DELETE_INDEX);
        return new DeleteIndexLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }


    public PutRowLauncher putRow(TraceLogger tracer, RetryStrategy retry, PutRowRequest originRequest)
    {
        Context ctx = contexts.get(OP_PUT_ROW);
        return new PutRowLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    
    public UpdateRowLauncher updateRow(TraceLogger tracer, RetryStrategy retry, UpdateRowRequest originRequest)
    {
        Context ctx = contexts.get(OP_UPDATE_ROW);
        return new UpdateRowLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    
    public DeleteRowLauncher deleteRow(TraceLogger tracer, RetryStrategy retry, DeleteRowRequest originRequest)
    {
        Context ctx = contexts.get(OP_DELETE_ROW);
        return new DeleteRowLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }
    
    public GetRowLauncher getRow(TraceLogger tracer, RetryStrategy retry, GetRowRequest originRequest)
    {
        Context ctx = contexts.get(OP_GET_ROW);
        return new GetRowLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public BatchGetRowLauncher batchGetRow(TraceLogger tracer, RetryStrategy retry, BatchGetRowRequest originRequest)
    {
        Context ctx = contexts.get(OP_BATCH_GET_ROW);
        return new BatchGetRowLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }
    
    public BatchWriteRowLauncher batchWriteRow(TraceLogger tracer, RetryStrategy retry, BatchWriteRowRequest originRequest)
    {
        Context ctx = contexts.get(OP_BATCH_WRITE_ROW);
        return new BatchWriteRowLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }
    
    public GetRangeLauncher getRange(TraceLogger tracer, RetryStrategy retry, GetRangeRequest originRequest)
    {
        Context ctx = contexts.get(OP_GET_RANGE);
        return new GetRangeLauncher(
        	ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }
    
    public ComputeSplitsBySizeLauncher computeSplitsBySize(TraceLogger tracer, RetryStrategy retry,
            ComputeSplitsBySizeRequest originRequest) {
        Context ctx = contexts.get(OP_COMPUTE_SPLITS_BY_SIZE);
        return new ComputeSplitsBySizeLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public ListStreamLauncher listStream(TraceLogger tracer, RetryStrategy retry, ListStreamRequest originRequest)
    {
        Context ctx = contexts.get(OP_LIST_STREAM);
        return new ListStreamLauncher(
                ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public DescribeStreamLauncher describeStream(TraceLogger tracer, RetryStrategy retry, DescribeStreamRequest originRequest)
    {
        Context ctx = contexts.get(OP_DESCRIBE_STREAM);
        return new DescribeStreamLauncher(
                ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public GetShardIteratorLauncher getShardIterator(TraceLogger tracer, RetryStrategy retry, GetShardIteratorRequest originRequest)
    {
        Context ctx = contexts.get(OP_GET_SHARD_ITERATOR);
        return new GetShardIteratorLauncher(
                ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public GetStreamRecordLauncher getStreamRecord(TraceLogger tracer, RetryStrategy retry, GetStreamRecordRequest originRequest)
    {
        Context ctx = contexts.get(OP_GET_STREAM_RECORD);
        return new GetStreamRecordLauncher(
                ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public StartLocalTransactionLauncher startLocalTransaction(TraceLogger tracer, RetryStrategy retry, StartLocalTransactionRequest originRequest)
    {
        Context ctx = contexts.get(OP_START_LOCAL_TRANSACTION);
        return new StartLocalTransactionLauncher(
                ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public CommitTransactionLauncher commitTransaction(TraceLogger tracer, RetryStrategy retry, CommitTransactionRequest originRequest)
    {
        Context ctx = contexts.get(OP_COMMIT_TRANSACTION);
        return new CommitTransactionLauncher(
                ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public AbortTransactionLauncher abortTransaction(TraceLogger tracer, RetryStrategy retry, AbortTransactionRequest originRequest)
    {
        Context ctx = contexts.get(OP_ABORT_TRANSACTION);
        return new AbortTransactionLauncher(
                ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public CreateSearchIndexLauncher createSearchIndex(TraceLogger tracer, RetryStrategy retry, CreateSearchIndexRequest originRequest)
    {
        Context ctx = contexts.get(OP_CREATE_SEARCH_INDEX);
        return new CreateSearchIndexLauncher(
                ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public ListSearchIndexLauncher listSearchIndex(TraceLogger tracer, RetryStrategy retry, ListSearchIndexRequest originRequest)
    {
        Context ctx = contexts.get(OP_LIST_SEARCH_INDEX);
        return new ListSearchIndexLauncher(
                ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public DeleteSearchIndexLauncher deleteSearchIndex(TraceLogger tracer, RetryStrategy retry, DeleteSearchIndexRequest originRequest)
    {
        Context ctx = contexts.get(OP_DELETE_SEARCH_INDEX);
        return new DeleteSearchIndexLauncher(
                ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public DescribeSearchIndexLauncher describeSearchIndex(TraceLogger tracer, RetryStrategy retry, DescribeSearchIndexRequest originRequest)
    {
        Context ctx = contexts.get(OP_DESCRIBE_SEARCH_INDEX);
        return new DescribeSearchIndexLauncher(
                ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public SearchLauncher search(TraceLogger tracer, RetryStrategy retry, SearchRequest originRequest)
    {
        Context ctx = contexts.get(OP_SEARCH);
        return new SearchLauncher(
                ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public CreateTunnelLauncher createTunnel(TraceLogger tracer, RetryStrategy retry,
                                             CreateTunnelRequest originRequest) {
        Context ctx = contexts.get(OP_CREATE_TUNNEL);
        return new CreateTunnelLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public ListTunnelLauncher listTunnel(TraceLogger tracer, RetryStrategy retry,
                                         ListTunnelRequest originRequest) {
        Context ctx = contexts.get(OP_LIST_TUNNEL);
        return new ListTunnelLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }


    public DescribeTunnelLauncher describeTunnel(TraceLogger tracer, RetryStrategy retry,
                                                 DescribeTunnelRequest originRequest) {
        Context ctx = contexts.get(OP_DESCRIBE_TUNNEL);
        return new DescribeTunnelLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public DeleteTunnelLauncher deleteTunnel(TraceLogger tracer, RetryStrategy retry,
                                             DeleteTunnelRequest originRequest) {
        Context ctx = contexts.get(OP_DELETE_TUNNEL);
        return new DeleteTunnelLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public ConnectTunnelLauncher connectTunnel(TraceLogger tracer, RetryStrategy retry,
                                               ConnectTunnelRequest originRequest) {
       Context ctx = contexts.get(OP_CONNECT_TUNNEL);
       return new ConnectTunnelLauncher(
           ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public HeartbeatLauncher heartbeat(TraceLogger tracer, RetryStrategy retry,
                                       HeartbeatRequest originRequest) {
        Context ctx = contexts.get(OP_HEARTBEAT);
        return new HeartbeatLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public ShutdownTunnelLauncher shutdownTunnel(TraceLogger tracer, RetryStrategy retry,
                                                 ShutdownTunnelRequest originRequest) {
       Context ctx = contexts.get(OP_SHUTDOWN_TUNNEL);
       return new ShutdownTunnelLauncher(
           ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public GetCheckpointLauncher getCheckpoint(TraceLogger tracer, RetryStrategy retry,
                                               GetCheckpointRequest originRequest) {
        Context ctx = contexts.get(OP_GETCHECKPOINT);
        return new GetCheckpointLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public ReadRecordsLauncher readRecords(TraceLogger tracer, RetryStrategy retry,
                                           ReadRecordsRequest originRequest) {
        Context ctx = contexts.get(OP_READRECORDS);
        return new ReadRecordsLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    public CheckpointLauncher checkpoint(TraceLogger tracer, RetryStrategy retry,
                                         CheckpointRequest originRequest) {
        Context ctx = contexts.get(OP_CHECKPOINT);
        return new CheckpointLauncher(
            ctx.uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }
}
