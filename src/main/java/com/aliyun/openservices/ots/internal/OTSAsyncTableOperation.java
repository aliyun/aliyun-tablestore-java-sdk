package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.OTSActionNames;
import com.aliyun.openservices.ots.OTSServiceConfiguration;
import com.aliyun.openservices.ots.auth.ServiceCredentials;
import com.aliyun.openservices.ots.comm.OTSUri;
import com.aliyun.openservices.ots.comm.ServiceClient;
import com.aliyun.openservices.ots.log.LogUtil;
import com.aliyun.openservices.ots.model.CreateTableRequest;
import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.model.DeleteTableRequest;
import com.aliyun.openservices.ots.model.DescribeTableRequest;
import com.aliyun.openservices.ots.model.ListTableRequest;
import com.aliyun.openservices.ots.model.UpdateTableRequest;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.*;

import java.net.URI;
import java.util.Map;

public class OTSAsyncTableOperation extends OTSOperation {
    private OTSUri URI_CREATE_TABLE;
    private OTSUri URI_LIST_TABLE;
    private OTSUri URI_DELETE_TABLE;
    private OTSUri URI_DESCRIBE_TABLE;
    private OTSUri URI_UPDATE_TABLE;

    public OTSAsyncTableOperation(String endpoint,
                                  String instanceName,
                                  ServiceClient client,
                                  ServiceCredentials credentials,
                                  OTSServiceConfiguration serviceConfig) {
        super(instanceName, client, credentials, serviceConfig);

        URI_CREATE_TABLE = new OTSUri(endpoint, OTSActionNames.ACTION_CREATE_TABLE);
        URI_LIST_TABLE = new OTSUri(endpoint, OTSActionNames.ACTION_LIST_TABLE);
        URI_DELETE_TABLE = new OTSUri(endpoint, OTSActionNames.ACTION_DELETE_TABLE);
        URI_DESCRIBE_TABLE = new OTSUri(endpoint, OTSActionNames.ACTION_DESCRIBE_TABLE);
        URI_UPDATE_TABLE = new OTSUri(endpoint, OTSActionNames.ACTION_UPDATE_TABLE);
    }

    public void createTable(final OTSExecutionContext<CreateTableRequest, CreateTableResult> executionContext) {

        LogUtil.logBeforeExecution(executionContext);

        asyncInvokePost(
                URI_CREATE_TABLE,
                null,
                OTSProtocolHelper.buildCreateTableRequest(executionContext.getRequest()),
                executionContext.getTraceLogger(),
                new CreateTableAsyncResponseConsumer(OTSResultParserFactory.createFactory().
                        createProtocolBufferResultParser(CreateTableResponse.getDefaultInstance(),
                                executionContext.getTraceLogger().getTraceId()),
                        executionContext.getTraceLogger()),
                executionContext.getAsyncClientCallback());
    }

    public void listTable(final OTSExecutionContext<ListTableRequest, ListTableResult> executionContext) {

        LogUtil.logBeforeExecution(executionContext);

        asyncInvokePost(
                URI_LIST_TABLE,
                null,
                OTSProtocolHelper.buildListTableRequest(),
                executionContext.getTraceLogger(),
                new ListTableAsyncResponseConsumer(OTSResultParserFactory.createFactory().
                        createProtocolBufferResultParser(ListTableResponse.getDefaultInstance(),
                                executionContext.getTraceLogger().getTraceId()),
                        executionContext.getTraceLogger()),
                executionContext.getAsyncClientCallback());
    }

    public void deleteTable(final OTSExecutionContext<DeleteTableRequest, DeleteTableResult> executionContext) {

        LogUtil.logBeforeExecution(executionContext);

        asyncInvokePost(
                URI_DELETE_TABLE,
                null,
                OTSProtocolHelper.buildDeleteTableRequest(executionContext.getRequest().getTableName()),
                executionContext.getTraceLogger(),
                new DeleteTableAsyncResponseConsumer(OTSResultParserFactory.createFactory().
                        createProtocolBufferResultParser(DeleteTableResponse.getDefaultInstance(),
                                executionContext.getTraceLogger().getTraceId()),
                        executionContext.getTraceLogger()),
                executionContext.getAsyncClientCallback());
    }

    public void describeTable(final OTSExecutionContext<DescribeTableRequest, DescribeTableResult> executionContext) {

        LogUtil.logBeforeExecution(executionContext);

        asyncInvokePost(
                URI_DESCRIBE_TABLE,
                null,
                OTSProtocolHelper.buildDescribeTableRequest(executionContext.getRequest().getTableName()),
                executionContext.getTraceLogger(),
                new DescribeTableAsyncResponseConsumer(OTSResultParserFactory.createFactory().
                        createProtocolBufferResultParser(DescribeTableResponse.getDefaultInstance(),
                                executionContext.getTraceLogger().getTraceId()),
                        executionContext.getTraceLogger()),
                executionContext.getAsyncClientCallback());
    }

    public void updateTable(final OTSExecutionContext<UpdateTableRequest, UpdateTableResult> executionContext) {

        LogUtil.logBeforeExecution(executionContext);

        asyncInvokePost(
                URI_UPDATE_TABLE,
                null,
                OTSProtocolHelper.buildUpdateTableRequest(executionContext.getRequest()),
                executionContext.getTraceLogger(),
                new UpdateTableAsyncResponseConsumer(OTSResultParserFactory.createFactory().
                        createProtocolBufferResultParser(UpdateTableResponse.getDefaultInstance(),
                                executionContext.getTraceLogger().getTraceId()),
                        executionContext.getTraceLogger()),
                executionContext.getAsyncClientCallback());
    }

}