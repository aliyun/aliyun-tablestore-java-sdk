package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentialProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AsyncGetRangeCompletionTest {

    static class GetRangeCompletion extends AsyncGetRangeCompletion {

        GetRangeResponse finalResult;

        public GetRangeCompletion(OperationLauncher launcher,
                                     GetRangeRequest request,
                                     TraceLogger tracer,
                                     ExecutorService callbackExecutor,
                                     RetryStrategy retry,
                                     ScheduledExecutorService retryExecutor) {
            super(launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        }

        @Override
        public void onCompleted(final GetRangeRequest req, final GetRangeResponse res) {
            finalResult = res;
        }
    }

    private GetRangeCompletion buildGetRangeCompletion(GetRangeRequest getRangeRequest) {
        TraceLogger traceLogger = new TraceLogger("traceId", 1000);
        RetryStrategy retryStrategy = new DefaultRetryStrategy();
        ExecutorService callbackExecutor = Executors.newFixedThreadPool(1);
        ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor();
        AsyncServiceClient client = new AsyncServiceClient(new ClientConfiguration());

        callbackExecutor.shutdown();
        retryExecutor.shutdown();
        client.shutdown();

        GetRangeCompletion completion = new GetRangeCompletion(new OperationLauncher<GetRangeRequest, GetRangeResponse>("", client,
                new DefaultCredentialProvider("accessid", "accesskey"), new ClientConfiguration(), getRangeRequest) {
            @Override
            public void fire(GetRangeRequest request, FutureCallback<GetRangeResponse> cb) {

            }
        }, getRangeRequest, traceLogger, callbackExecutor, retryStrategy, retryExecutor);
        return completion;
    }

    private List<Column> generateColumn(int colNum, int verNum) {
        Random random = new Random();
        List<Column> columns = new ArrayList<Column>();
        for (int i = 0; i < colNum; i++) {
            for (int j = 0; j < verNum; j++) {
                columns.add(new Column("Col" + i, ColumnValue.fromLong(random.nextInt()), j));
            }
        }
        return columns;
    }

    @Test
    public void testMoreThanOneRow() {
        GetRangeRequest getRangeRequest = new GetRangeRequest();
        GetRangeCompletion completion = buildGetRangeCompletion(getRangeRequest);
        GetRangeResponse getRangeResponse = new GetRangeResponse(new Response("reqId"), new ConsumedCapacity(new CapacityUnit(1, 1)));
        List<Row> rows = new ArrayList<Row>();

        PrimaryKey primaryKey = new PrimaryKey(new PrimaryKeyColumn[] {new PrimaryKeyColumn("PK", PrimaryKeyValue.fromLong(1))});
        Row row1 = new Row(primaryKey, generateColumn(10, 10));
        primaryKey = new PrimaryKey(new PrimaryKeyColumn[]{new PrimaryKeyColumn("PK", PrimaryKeyValue.fromLong(2))});
        Row row2 = new Row(primaryKey, generateColumn(10, 10));
        rows.add(row1);
        rows.add(row2);
        getRangeResponse.setRows(rows);
        getRangeResponse.setNextToken("token".getBytes());

        completion.completed(getRangeResponse);
        assertEquals(2, completion.finalResult.getRows().size());
        assertEquals(row1, completion.finalResult.getRows().get(0));
        assertEquals(row2, completion.finalResult.getRows().get(1));

        completion = buildGetRangeCompletion(getRangeRequest);
        getRangeResponse.setNextStartPrimaryKey(row2.getPrimaryKey());
        completion.completed(getRangeResponse);
        assertEquals(1, completion.finalResult.getRows().size());
        assertEquals(row1, completion.finalResult.getRows().get(0));
    }

    @Test
    public void testBuildNextRequest() {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria("Table");
        PrimaryKey primaryKey = new PrimaryKey(new PrimaryKeyColumn[] {new PrimaryKeyColumn("PK", PrimaryKeyValue.fromLong(1))});
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(primaryKey);
        primaryKey = new PrimaryKey(new PrimaryKeyColumn[] {new PrimaryKeyColumn("PK", PrimaryKeyValue.fromLong(2))});
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(primaryKey);
        rangeRowQueryCriteria.setMaxVersions(10);
        rangeRowQueryCriteria.setTimeRange(new TimeRange(0, 100));
        rangeRowQueryCriteria.setFilter(new SingleColumnValueFilter("Col", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(1)));
        rangeRowQueryCriteria.setLimit(10);
        rangeRowQueryCriteria.setDirection(Direction.BACKWARD);
        rangeRowQueryCriteria.setStartColumn("Col1");
        rangeRowQueryCriteria.setEndColumn("Col2");
        GetRangeRequest getRangeRequest = new GetRangeRequest(rangeRowQueryCriteria);

        GetRangeCompletion completion = buildGetRangeCompletion(getRangeRequest);
        primaryKey = new PrimaryKey(new PrimaryKeyColumn[] {new PrimaryKeyColumn("PK", PrimaryKeyValue.fromLong(3))});
        completion.buildNextRequest(primaryKey, "token".getBytes());

        GetRangeRequest nextRequest = completion.nextRequest;

        assertEquals(primaryKey, nextRequest.getRangeRowQueryCriteria().getInclusiveStartPrimaryKey());
        assertEquals(rangeRowQueryCriteria.getExclusiveEndPrimaryKey(), nextRequest.getRangeRowQueryCriteria().getExclusiveEndPrimaryKey());
        assertEquals(10, nextRequest.getRangeRowQueryCriteria().getMaxVersions());
        assertEquals(rangeRowQueryCriteria.getTimeRange(), nextRequest.getRangeRowQueryCriteria().getTimeRange());
        assertTrue(nextRequest.getRangeRowQueryCriteria().getFilter() instanceof SingleColumnValueFilter);
        assertEquals(10, nextRequest.getRangeRowQueryCriteria().getLimit());
        assertEquals(Direction.BACKWARD, nextRequest.getRangeRowQueryCriteria().getDirection());
        assertEquals("Col1", nextRequest.getRangeRowQueryCriteria().getStartColumn());
        assertEquals("Col2", nextRequest.getRangeRowQueryCriteria().getEndColumn());
        assertTrue(Arrays.equals("token".getBytes(), nextRequest.getRangeRowQueryCriteria().getToken()));
    }

    @Test
    public void testHandleResult() {
        GetRangeRequest getRangeRequest = new GetRangeRequest();
        GetRangeCompletion completion = buildGetRangeCompletion(getRangeRequest);
        GetRangeResponse getRangeResponse = new GetRangeResponse(new Response("reqId"), new ConsumedCapacity(new CapacityUnit(1, 1)));
        List<Row> rows = new ArrayList<Row>();

        PrimaryKey primaryKey = new PrimaryKey(new PrimaryKeyColumn[] {new PrimaryKeyColumn("PK", PrimaryKeyValue.fromLong(1))});
        Row row1 = new Row(primaryKey, generateColumn(10, 10));
        primaryKey = new PrimaryKey(new PrimaryKeyColumn[]{new PrimaryKeyColumn("PK", PrimaryKeyValue.fromLong(2))});
        Row row2 = new Row(primaryKey, generateColumn(10, 10));
        rows.add(row1);
        rows.add(row2);
        getRangeResponse.setRows(rows);
        getRangeResponse.setNextToken("token".getBytes());

        completion.handleResult(getRangeResponse);
        assertEquals(2, completion.completeRows.size());
        assertEquals(row1, completion.completeRows.get(0));
        assertEquals(row2, completion.completeRows.get(1));

        completion = buildGetRangeCompletion(getRangeRequest);

        getRangeResponse.setNextStartPrimaryKey(row2.getPrimaryKey());
        completion.handleResult(getRangeResponse);
        assertEquals(1, completion.completeRows.size());
        assertEquals(row1, completion.completeRows.get(0));
        assertEquals(row2.getPrimaryKey(), completion.lastRowPK);
        assertEquals(100, completion.lastRowColumns.size());
    }
}
