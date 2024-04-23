package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentialProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.model.*;
import org.apache.http.concurrent.FutureCallback;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;

public class AsyncGetRowCompletionTest {

    static class GetRowCompletion extends AsyncGetRowCompletion {

        public GetRowCompletion(OperationLauncher<GetRowRequest, GetRowResponse> launcher,
                                GetRowRequest request,
                                TraceLogger tracer,
                                ExecutorService callbackExecutor,
                                RetryStrategy retry,
                                ScheduledExecutorService retryExecutor) {
            super(launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        }

        @Override
        public void onCompleted(final GetRowRequest req, final GetRowResponse res) {
        }
    }

    private GetRowCompletion buildGetRowCompletion() {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("PK", PrimaryKeyValue.fromLong(1));
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria("Table", primaryKeyBuilder.build());
        GetRowRequest getRowRequest = new GetRowRequest(criteria);
        TraceLogger traceLogger = new TraceLogger("traceId", 1000);
        RetryStrategy retryStrategy = new DefaultRetryStrategy();
        ExecutorService callbackExecutor = Executors.newFixedThreadPool(1);
        ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor();
        AsyncServiceClient client = new AsyncServiceClient(new ClientConfiguration());

        callbackExecutor.shutdown();
        retryExecutor.shutdown();
        client.shutdown();

        GetRowCompletion completion = new GetRowCompletion(new OperationLauncher<GetRowRequest, GetRowResponse>("", client,
                new DefaultCredentialProvider("accessid", "accesskey"), new ClientConfiguration(), getRowRequest) {
            @Override
            public void fire(GetRowRequest request, FutureCallback<GetRowResponse> cb) {

            }
        }, getRowRequest, traceLogger, callbackExecutor, retryStrategy, retryExecutor);
        return completion;
    }

    private List<GetRowResponse> generateResponseList(Row row) {
        Random random = new Random();
        List<GetRowResponse> responseList = new ArrayList<GetRowResponse>();
        Response response = new Response("requestId");
        if (row == null) {
            while (random.nextBoolean()) {
                GetRowResponse getRowResponse = new GetRowResponse(response, null, new ConsumedCapacity(new CapacityUnit(1, 0)));
                getRowResponse.setNextToken("token".getBytes());
                responseList.add(getRowResponse);
            }
            responseList.add(new GetRowResponse(response, null, new ConsumedCapacity(new CapacityUnit(1, 0))));
            return responseList;
        } else {
            PrimaryKey primaryKey = row.getPrimaryKey();
            List<Column> columns = Arrays.asList(row.getColumns());
            while (random.nextBoolean()) {
                GetRowResponse getRowResponse = new GetRowResponse(response, null, new ConsumedCapacity(new CapacityUnit(1, 0)));
                getRowResponse.setNextToken("token".getBytes());
                responseList.add(getRowResponse);
            }
            int cur = 0;
            boolean finished = false;
            while (!finished) {
                int size = random.nextInt(columns.size() + 1 - cur);
                Row rowSlice = null;
                if (size > 0) {
                    rowSlice = new Row(primaryKey, columns.subList(cur, cur + size));
                }
                GetRowResponse getRowResponse = new GetRowResponse(response, rowSlice, new ConsumedCapacity(new CapacityUnit(1, 0)));
                cur += size;
                if (cur == columns.size()) {
                    finished = random.nextBoolean();
                }
                if (!finished) {
                    getRowResponse.setNextToken("token".getBytes());
                } else if (columns.isEmpty()) {
                    rowSlice = new Row(primaryKey, new ArrayList<Column>());
                    getRowResponse = new GetRowResponse(response, rowSlice, new ConsumedCapacity(new CapacityUnit(1, 0)));
                }
                responseList.add(getRowResponse);
            }
            return responseList;
        }
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
    public void testHandleResult() {
        for (int i = 0; i < 1000; i++) {
            GetRowCompletion completion = buildGetRowCompletion();
            List<GetRowResponse> responseList = generateResponseList(null);
            for (GetRowResponse getRowResponse : responseList) {
                completion.handleResult(getRowResponse);
            }
            GetRowResponse getRowResponse = completion.buildFinalResult();
            assertEquals(null, getRowResponse.getRow());
        }

        for (int i = 0; i < 1000; i++) {
            GetRowCompletion completion = buildGetRowCompletion();
            Random random = new Random();
            PrimaryKey primaryKey = new PrimaryKey(new PrimaryKeyColumn[] {new PrimaryKeyColumn("PK", PrimaryKeyValue.fromLong(1))});
            List<Column> columns = generateColumn(random.nextInt(100), random.nextInt(10) + 1);
            Row row = new Row(primaryKey, columns);
            List<GetRowResponse> responseList = generateResponseList(row);
            for (GetRowResponse getRowResponse : responseList) {
                completion.handleResult(getRowResponse);
            }
            GetRowResponse getRowResponse = completion.buildFinalResult();
            assertEquals(row.toString(), getRowResponse.getRow().toString());
        }
    }
}
