package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentialProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.Error;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AsyncBatchGetRowCompletionTest {

    static class BatchGetRowCompletion extends AsyncBatchGetRowCompletion {
        public BatchGetRowCompletion(OperationLauncher launcher,
                                     BatchGetRowRequest request,
                                     TraceLogger tracer,
                                     ExecutorService callbackExecutor,
                                     RetryStrategy retry,
                                     ScheduledExecutorService retryExecutor) {
            super(launcher, request, tracer, callbackExecutor, retry, retryExecutor);
        }

        @Override
        public void onCompleted(final BatchGetRowRequest req, final BatchGetRowResponse res) {
        }
    }

    private BatchGetRowCompletion buildBatchGetRowCompletion(BatchGetRowRequest batchGetRowRequest) {
        TraceLogger traceLogger = new TraceLogger("traceId", 1000);
        RetryStrategy retryStrategy = new DefaultRetryStrategy();
        ExecutorService callbackExecutor = Executors.newFixedThreadPool(1);
        ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor();
        AsyncServiceClient client = new AsyncServiceClient(new ClientConfiguration());

        callbackExecutor.shutdown();
        retryExecutor.shutdown();
        client.shutdown();

        BatchGetRowCompletion completion = new BatchGetRowCompletion(new OperationLauncher<BatchGetRowRequest, BatchGetRowResponse>("", client,
                new DefaultCredentialProvider("accessid", "accesskey"), new ClientConfiguration(), batchGetRowRequest) {
            @Override
            public void fire(BatchGetRowRequest request, FutureCallback<BatchGetRowResponse> cb) {

            }
        }, batchGetRowRequest, traceLogger, callbackExecutor, retryStrategy, retryExecutor);
        return completion;
    }

    private BatchGetRowRequest generateBatchGetRowRequest(int tableNum, int rowPerTable) {
        BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
        for (int i = 0; i < tableNum; i++) {
            MultiRowQueryCriteria criteria = new MultiRowQueryCriteria("Table" + i);
            for (int j = 0; j < rowPerTable; j++) {
                PrimaryKey primaryKey = new PrimaryKey(new PrimaryKeyColumn[] {new PrimaryKeyColumn("PK", PrimaryKeyValue.fromLong(j))});
                criteria.addRow(primaryKey);
            }
            criteria.setMaxVersions(i + 1); // test set parameters for next request.
            batchGetRowRequest.addMultiRowQueryCriteria(criteria);
        }
        return batchGetRowRequest;
    }

    private List<GetRowResponse> generateRowSlices(Row row) {
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
    public void testOneRow() {
        for (int t = 0; t < 100; t++) {
            PrimaryKey primaryKey = new PrimaryKey(new PrimaryKeyColumn[]{new PrimaryKeyColumn("PK", PrimaryKeyValue.fromLong(0))});
            List<Column> columns = generateColumn(10, 10);
            Row row = new Row(primaryKey, columns);
            BatchGetRowRequest batchGetRowRequest = generateBatchGetRowRequest(1, 1);
            BatchGetRowCompletion completion = buildBatchGetRowCompletion(batchGetRowRequest);

            List<GetRowResponse> getRowResponses = generateRowSlices(row);
            for (GetRowResponse getRowResponse : getRowResponses) {
                BatchGetRowResponse batchGetRowResponse = new BatchGetRowResponse(new Response("requestId"));
                batchGetRowResponse.addResult(new BatchGetRowResponse.RowResult("Table0", getRowResponse.getRow(), new ConsumedCapacity(new CapacityUnit(1, 0)), 0, getRowResponse.getNextToken()));
                completion.handleResult(batchGetRowResponse);
            }

            BatchGetRowResponse batchGetRowResponse = completion.buildFinalResult();
            assertEquals(1, batchGetRowResponse.getTableToRowsResult().size());
            assertEquals(1, batchGetRowResponse.getBatchGetRowResult("Table0").size());
            BatchGetRowResponse.RowResult rowResult = batchGetRowResponse.getBatchGetRowResult("Table0").get(0);
            assertEquals(row.toString(), rowResult.getRow().toString());
            assertTrue(!rowResult.hasNextToken());
        }
    }

    @Test
    public void testMultiTableWithMultiRow() {
        for (int t = 0; t < 10; t++) {
            Map<String, Map<PrimaryKey, Iterator<GetRowResponse>>> allResults = new HashMap<String, Map<PrimaryKey, Iterator<GetRowResponse>>>();
            Map<String, Map<PrimaryKey, Row>> finalResults = new HashMap<String, Map<PrimaryKey, Row>>();

            for (int i = 0; i < 10; i++) {
                Map<PrimaryKey, Iterator<GetRowResponse>> tableResults = new HashMap<PrimaryKey, Iterator<GetRowResponse>>();
                Map<PrimaryKey, Row> tableRows = new HashMap<PrimaryKey, Row>();
                for (int j = 0; j < 10; j++) {
                    PrimaryKey primaryKey = new PrimaryKey(new PrimaryKeyColumn[]{new PrimaryKeyColumn("PK", PrimaryKeyValue.fromLong(j))});
                    List<Column> columns = generateColumn(10, 10);
                    Row row = new Row(primaryKey, columns);
                    tableResults.put(primaryKey, generateRowSlices(row).iterator());
                    tableRows.put(primaryKey, row);
                }
                allResults.put("Table" + i, tableResults);
                finalResults.put("Table" + i, tableRows);
            }

            BatchGetRowRequest batchGetRowRequest = generateBatchGetRowRequest(10, 10);
            BatchGetRowCompletion completion = buildBatchGetRowCompletion(batchGetRowRequest);

            while (!batchGetRowRequest.isEmpty()) {
                BatchGetRowResponse response = new BatchGetRowResponse(new Response("requestId"));
                for (String table : batchGetRowRequest.getCriteriasByTable().keySet()) {
                    MultiRowQueryCriteria criteria = batchGetRowRequest.getCriteria(table);
                    for (int i = 0; i < criteria.getRowKeys().size(); i++) {
                        PrimaryKey primaryKey = criteria.get(i);
                        GetRowResponse getRowResponse = allResults.get(table).get(primaryKey).next();
                        response.addResult(new BatchGetRowResponse.RowResult(table, getRowResponse.getRow(), getRowResponse.getConsumedCapacity(), i, getRowResponse.getNextToken()));
                    }
                }
                completion.handleResult(response);
                completion.buildNextRequest();
                batchGetRowRequest = completion.nextRequest;
            }
            BatchGetRowResponse batchGetRowResponse = completion.buildFinalResult();
            assertEquals(100, batchGetRowResponse.getSucceedRows().size());
            assertEquals(0, batchGetRowResponse.getFailedRows().size());
            for (BatchGetRowResponse.RowResult rowResult : batchGetRowResponse.getSucceedRows()) {
                Row expect = finalResults.get(rowResult.getTableName()).get(rowResult.getRow().getPrimaryKey());
                assertEquals(expect.toString(), rowResult.getRow().toString());
            }
        }
    }

    @Test
    public void testBuildNextRequest() {
        Random random = new Random();
        for (int t = 0; t < 100; t++) {
            BatchGetRowRequest batchGetRowRequest = generateBatchGetRowRequest(10, 10);
            BatchGetRowCompletion completion = buildBatchGetRowCompletion(batchGetRowRequest);

            BatchGetRowResponse response = new BatchGetRowResponse(new Response("requestId"));
            BatchGetRowRequest nextRequest = new BatchGetRowRequest();
            for (int i = 0; i < 10; i++) {
                MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria("Table" + i);
                multiRowQueryCriteria.setMaxVersions(i + 1); // test set parameters for next request.
                for (int j = 0; j < 10; j++) {
                    BatchGetRowResponse.RowResult rowResult;
                    if (random.nextBoolean()) {
                        if (random.nextBoolean()) {
                            rowResult = new BatchGetRowResponse.RowResult("Table" + i, null, new ConsumedCapacity(new CapacityUnit(1, 1)), j, ("token" + i + j).getBytes());
                            multiRowQueryCriteria.addRow(batchGetRowRequest.getPrimaryKey("Table" + i, j), ("token" + i + j).getBytes());
                        } else {
                            rowResult = new BatchGetRowResponse.RowResult("Table" + i, null, new ConsumedCapacity(new CapacityUnit(1, 1)), j);
                        }
                    } else {
                        rowResult = new BatchGetRowResponse.RowResult("Table" + i, new Error("", ""), j);
                    }
                    response.addResult(rowResult);
                }
                if (!multiRowQueryCriteria.isEmpty()) {
                    nextRequest.addMultiRowQueryCriteria(multiRowQueryCriteria);
                }
            }
            completion.handleResult(response);
            completion.buildNextRequest();

            assertEquals(nextRequest.getCriteriasByTable().size(), completion.nextRequest.getCriteriasByTable().size());
            for (String tableName : nextRequest.getCriteriasByTable().keySet()) {
                MultiRowQueryCriteria expectCriteria = nextRequest.getCriteria(tableName);
                MultiRowQueryCriteria criteria = completion.nextRequest.getCriteria(tableName);
                assertEquals(expectCriteria.getRowKeys().size(), criteria.getRowKeys().size());
                assertEquals(expectCriteria.getMaxVersions(), criteria.getMaxVersions()); // test set parameters in next request.
                for (int i = 0; i < expectCriteria.getRowKeys().size(); i++) {
                    assertEquals(expectCriteria.getRowKeys().get(i), criteria.getRowKeys().get(i));
                    assertTrue(Arrays.equals(expectCriteria.getTokens().get(i), criteria.getTokens().get(i)));
                }
            }
        }
    }
}
