package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.OTSActionNames;
import com.aliyun.openservices.ots.OTSServiceConfiguration;
import com.aliyun.openservices.ots.auth.ServiceCredentials;
import com.aliyun.openservices.ots.comm.OTSUri;
import com.aliyun.openservices.ots.comm.ServiceClient;
import com.aliyun.openservices.ots.log.LogUtil;
import com.aliyun.openservices.ots.model.DeleteRowRequest;
import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.model.GetRangeRequest;
import com.aliyun.openservices.ots.model.GetRowRequest;
import com.aliyun.openservices.ots.model.PutRowRequest;
import com.aliyun.openservices.ots.model.UpdateRowRequest;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.*;

import java.util.Map;

public class OTSAsyncDataOperation extends OTSOperation {
    private OTSUri URI_GET_ROW;
    private OTSUri URI_PUT_ROW;
    private OTSUri URI_UPDATE_ROW;
    private OTSUri URI_DELETE_ROW;
    private OTSUri URI_BATCH_GET_ROW;
    private OTSUri URI_BATCH_WRITE_ROW;
    private OTSUri URI_GET_RANGE;

    public OTSAsyncDataOperation(String endpoint,
                                 String instanceName,
                                 ServiceClient client,
                                 ServiceCredentials credentials,
                                 OTSServiceConfiguration serviceConfig) {
        super(instanceName, client, credentials, serviceConfig);

        URI_GET_ROW = new OTSUri(endpoint, OTSActionNames.ACTION_GET_ROW);
        URI_PUT_ROW = new OTSUri(endpoint, OTSActionNames.ACTION_PUT_ROW);
        URI_UPDATE_ROW = new OTSUri(endpoint, OTSActionNames.ACTION_UPDATE_ROW);
        URI_DELETE_ROW = new OTSUri(endpoint, OTSActionNames.ACTION_DELETE_ROW);
        URI_BATCH_GET_ROW = new OTSUri(endpoint, OTSActionNames.ACTION_BATCH_GET_ROW);
        URI_BATCH_WRITE_ROW = new OTSUri(endpoint, OTSActionNames.ACTION_BATCH_WRITE_ROW);
        URI_GET_RANGE = new OTSUri(endpoint, OTSActionNames.ACTION_GET_RANGE);
    }

    public void getRow(final OTSExecutionContext<GetRowRequest, GetRowResult> executionContext) {

        SingleRowQueryCriteria criteria = executionContext.getRequest().getRowQueryCriteria();

        LogUtil.logBeforeExecution(executionContext);

        asyncInvokePost(
                URI_GET_ROW,
                null,
                OTSProtocolHelper.buildGetRowRequest(criteria),
                executionContext.getTraceLogger(),
                new GetRowAsyncResponseConsumer(OTSResultParserFactory.createFactory().
                        createProtocolBufferResultParser(GetRowResponse.getDefaultInstance(),
                                executionContext.getTraceLogger().getTraceId()),
                        executionContext.getTraceLogger()),
                executionContext.getAsyncClientCallback());
    }

    public void putRow(final OTSExecutionContext<PutRowRequest, PutRowResult> executionContext) {

        RowPutChange rowChange = executionContext.getRequest().getRowChange();

        LogUtil.logBeforeExecution(executionContext);

        asyncInvokePost(
                URI_PUT_ROW,
                null,
                OTSProtocolHelper.buildPutRowRequest(rowChange),
                executionContext.getTraceLogger(),
                new PutRowAsyncResponseConsumer(OTSResultParserFactory.createFactory().
                        createProtocolBufferResultParser(PutRowResponse.getDefaultInstance(),
                                executionContext.getTraceLogger().getTraceId()),
                        executionContext.getTraceLogger()),
                executionContext.getAsyncClientCallback());
    }

    public void updateRow(final OTSExecutionContext<UpdateRowRequest, UpdateRowResult> executionContext) {

        RowUpdateChange rowChange = executionContext.getRequest().getRowChange();

        LogUtil.logBeforeExecution(executionContext);

        asyncInvokePost(
                URI_UPDATE_ROW,
                null,
                OTSProtocolHelper.buildUpdateRowRequest(rowChange),
                executionContext.getTraceLogger(),
                new UpdateRowAsyncResponseConsumer(OTSResultParserFactory.createFactory().
                        createProtocolBufferResultParser(UpdateRowResponse.getDefaultInstance(),
                                executionContext.getTraceLogger().getTraceId()),
                        executionContext.getTraceLogger()),
                executionContext.getAsyncClientCallback());
    }

    public void deleteRow(final OTSExecutionContext<DeleteRowRequest, DeleteRowResult> executionContext) {

        RowDeleteChange rowChange = executionContext.getRequest().getRowChange();

        LogUtil.logBeforeExecution(executionContext);

        asyncInvokePost(
                URI_DELETE_ROW,
                null,
                OTSProtocolHelper.buildDeleteRowRequest(rowChange),
                executionContext.getTraceLogger(),
                new DeleteRowAsyncResponseConsumer(OTSResultParserFactory.createFactory().
                        createProtocolBufferResultParser(DeleteRowResponse.getDefaultInstance(),
                                executionContext.getTraceLogger().getTraceId()),
                        executionContext.getTraceLogger()),
                executionContext.getAsyncClientCallback());
    }

    public void batchGetRow(final BatchGetRowExecutionContext executionContext) {

        Map<String, MultiRowQueryCriteria> criteriasGroupByTable = executionContext.getRequest().getCriteriasByTable();

        LogUtil.logBeforeExecution(executionContext);

        asyncInvokePost(
                URI_BATCH_GET_ROW,
                null,
                OTSProtocolHelper.buildBatchGetRowRequest(criteriasGroupByTable),
                executionContext.getTraceLogger(),
                new BatchGetRowAsyncResponseConsumer(OTSResultParserFactory.createFactory().
                        createProtocolBufferResultParser(BatchGetRowResponse.getDefaultInstance(),
                                executionContext.getTraceLogger().getTraceId()),
                        executionContext),
                executionContext.getAsyncClientCallback());
    }

    public void batchWriteRow(final BatchWriteRowExecutionContext executionContext) {

        LogUtil.logBeforeExecution(executionContext);

        asyncInvokePost(
                URI_BATCH_WRITE_ROW,
                null,
                OTSProtocolHelper.buildBatchWriteRowRequest(executionContext.getRequest()),
                executionContext.getTraceLogger(),
                new BatchWriteRowAsyncResponseConsumer(OTSResultParserFactory.createFactory().
                        createProtocolBufferResultParser(BatchWriteRowResponse.getDefaultInstance(),
                                executionContext.getTraceLogger().getTraceId()),
                        executionContext),
                executionContext.getAsyncClientCallback());
    }

    public void getRange(final OTSExecutionContext<GetRangeRequest, GetRangeResult> executionContext) {

        LogUtil.logBeforeExecution(executionContext);

        RangeRowQueryCriteria criteria = executionContext.getRequest().getRangeRowQueryCriteria();

        asyncInvokePost(
                URI_GET_RANGE,
                null,
                OTSProtocolHelper.buildGetRangeRequest(criteria),
                executionContext.getTraceLogger(),
                new GetRangeAsyncResponseConsumer(OTSResultParserFactory.createFactory().
                        createProtocolBufferResultParser(GetRangeResponse.getDefaultInstance(),
                                executionContext.getTraceLogger().getTraceId()),
                        executionContext.getTraceLogger()),
                executionContext.getAsyncClientCallback());
    }
}

