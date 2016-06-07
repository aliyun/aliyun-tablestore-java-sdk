package examples;

import com.aliyun.openservices.ots.*;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.model.condition.RelationalCondition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class OTSAsyncSample {

    private static final String COLUMN_GID_NAME = "gid";
    private static final String COLUMN_UID_NAME = "uid";
    private static final String COLUMN_NAME_NAME = "name";
    private static final String COLUMN_AGE_NAME = "age";

    public static void main(String args[]) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";
        
        OTSClient client = new OTSClient(endPoint, accessId, accessKey, instanceName);
        OTSClientAsync asyncClient = new OTSClientAsync(endPoint, accessId, accessKey, instanceName);
        final String tableName = "sampleTable";

        try{
            // 创建表
            createTable(client, tableName);

            // 注意：创建表只是提交请求，OTS创建表需要一段时间。
            // 这里简单地等待2秒，请根据您的实际逻辑修改。
            Thread.sleep(2000);

            listTableWithFuture(asyncClient);
            listTableWithCallback(asyncClient);

            // 异步并发的执行多次batchWriteRow操作
            batchWriteRow(asyncClient, tableName);

            // 异步并发的执行多次getRange操作
            batchGetRange(asyncClient, tableName);
        }catch(ServiceException e){
            System.err.println("操作失败，详情：" + e.getMessage());
            // 可以根据错误代码做出处理， OTS的ErrorCode定义在OTSErrorCode中。
            if (OTSErrorCode.QUOTA_EXHAUSTED.equals(e.getErrorCode())){
                System.err.println("超出存储配额。");
            }
            // Request ID可以用于有问题时联系客服诊断异常。
            System.err.println("Request ID:" + e.getRequestId());
        }catch(ClientException e){
            // 可能是网络不好或者是返回结果有问题
            System.err.println("请求失败，详情：" + e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        }
        finally{
            // 不留垃圾。
            try {
                deleteTable(client, tableName);
            } catch (ServiceException e) {
                System.err.println("删除表格失败，原因：" + e.getMessage());
                e.printStackTrace();
            } catch (ClientException e) {
                System.err.println("删除表格请求失败，原因：" + e.getMessage());
                e.printStackTrace();
            }
            client.shutdown();
            asyncClient.shutdown();
        }
    }

    private static OTSFuture<GetRangeResult> sendGetRangeRequest(OTSClientAsync asyncClient, String tableName, long start, long end) {
        RowPrimaryKey startPk = new RowPrimaryKey();
        startPk.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.fromLong(start));
        startPk.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.INF_MIN);

        RowPrimaryKey endPk = new RowPrimaryKey();
        endPk.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.fromLong(end));
        endPk.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.INF_MIN);

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(startPk);
        criteria.setExclusiveEndPrimaryKey(endPk);
        criteria.setLimit(10);

        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);
        OTSFuture<GetRangeResult> future = asyncClient.getRange(request);
        return future;
    }

    private static void batchGetRange(OTSClientAsync asyncClient, String tableName) {
        // 一次性查询多个范围的数据，设置10个任务，每个任务查询100条数据。
        // 每个范围查询的时候设置limit为10，100条数据需要10次请求才能全部查完。
        int count = 10;
        OTSFuture<GetRangeResult>[] futures = new OTSFuture[count];
        for (int i = 0; i < count; i++) {
            futures[i] = sendGetRangeRequest(asyncClient, tableName, i * 100, i * 100 + 100);
        }

        // 检查是否所有范围查询均已做完，若未做完，则继续发送查询请求
        List<Row> allRows = new ArrayList<Row>();
        while (true) {
            boolean completed = true;
            for (int i = 0; i < futures.length; i++) {
                OTSFuture<GetRangeResult> future = futures[i];
                if (future == null) {
                    continue;
                }

                if (future.isDone()) {
                    GetRangeResult result = future.get();
                    allRows.addAll(result.getRows());

                    if (result.getNextStartPrimaryKey() != null) {
                        // 该范围还未查询完毕，需要从nextStart开始继续往下读。
                        long nextStart = result.getNextStartPrimaryKey().getPrimaryKey().get(COLUMN_GID_NAME).asLong();
                        long rangeEnd = i * 100 + 100;
                        futures[i] = sendGetRangeRequest(asyncClient, tableName, nextStart, rangeEnd);
                        completed = false;
                    } else {
                        futures[i] = null; // 若某个范围查询完毕，则将对应future设置为null
                    }
                } else {
                    completed = false;
                }
            }

            if (completed) {
                break;
            } else {
                try {
                    Thread.sleep(10); // 避免busy wait，每次循环完毕后等待一小段时间
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // 所有数据全部读出
        System.out.println("Total rows scanned: " + allRows.size());
    }

    private static void batchWriteRow(OTSClientAsync asyncClient, String tableName) {
        // BatchWriteRow的行数限制是100行，使用异步接口，实现一次批量导入1000行。
        List<OTSFuture<BatchWriteRowResult>> futures = new ArrayList<OTSFuture<BatchWriteRowResult>>();
        int count = 10;
        // 一次性发出10个请求，每个请求写100行数据
        for (int i = 0; i < count; i++) {
            BatchWriteRowRequest request = new BatchWriteRowRequest();
            for (int j = 0; j < 100; j++) {
                RowPutChange rowChange = new RowPutChange(tableName);
                RowPrimaryKey primaryKey = new RowPrimaryKey();
                primaryKey.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.fromLong(i * 100 + j));
                primaryKey.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.fromLong(j));
                rowChange.setPrimaryKey(primaryKey);
                rowChange.addAttributeColumn(COLUMN_NAME_NAME, ColumnValue.fromString("name" + j));
                rowChange.addAttributeColumn(COLUMN_AGE_NAME, ColumnValue.fromLong(j));

                request.addRowChange(rowChange);
            }
            OTSFuture<BatchWriteRowResult> result = asyncClient.batchWriteRow(request);
            futures.add(result);
        }

        // 等待结果返回
        List<BatchWriteRowResult> results = new ArrayList<BatchWriteRowResult>();
        for (OTSFuture<BatchWriteRowResult> future : futures) {
            try {
                BatchWriteRowResult result = future.get(); // 同步等待结果返回
                results.add(result);
            } catch (OTSException e) {
                e.printStackTrace();
            } catch (ClientException e) {
                e.printStackTrace();
            }
        }

        // 统计返回结果
        int totalSucceedRows = 0;
        int totalFailedRows = 0;
        for (BatchWriteRowResult result : results) {
            totalSucceedRows += result.getSucceedRowsOfPut().size();
            totalFailedRows += result.getFailedRowsOfPut().size();
        }

        System.out.println("Total succeed rows: " + totalSucceedRows);
        System.out.println("Total failed rows: " + totalFailedRows);
    }

    private static void listTableWithCallback(OTSClientAsync asyncClient) {
        final AtomicBoolean isDone = new AtomicBoolean(false);
        OTSCallback<ListTableRequest, ListTableResult> callback = new OTSCallback<ListTableRequest, ListTableResult>() {
            @Override
            public void onCompleted(OTSContext<ListTableRequest, ListTableResult> otsContext) {
                isDone.set(true);
                System.out.println("\nList table by listTableWithCallback:");
                for (String tableName : otsContext.getOTSResult().getTableNames()) {
                    System.out.println(tableName);
                }
            }

            @Override
            public void onFailed(OTSContext<ListTableRequest, ListTableResult> otsContext, OTSException ex) {
                isDone.set(true);
                ex.printStackTrace();
            }

            @Override
            public void onFailed(OTSContext<ListTableRequest, ListTableResult> otsContext, ClientException ex) {
                isDone.set(true);
                ex.printStackTrace();
            }
        };

        asyncClient.listTable(callback); // 将callback扔给SDK，SDK在完成请求接到响应后，会自动调用callback

        // 等待callback被调用，一般的业务处理逻辑下，不需要这一步等待。
        while (!isDone.get()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void listTableWithFuture(OTSClientAsync client) {
        // 通过Future同步的等待结果返回。
        try {
            OTSFuture<ListTableResult> future = client.listTable();
            ListTableResult result = future.get(); // 同步的等待
            System.out.println("\nList table by listTableWithFuture:");
            for (String tableName : result.getTableNames()) {
                System.out.println(tableName);
            }
        } catch (OTSException e) {
            e.printStackTrace();
        } catch (ClientException e) {
            e.printStackTrace();
        }

        // 通过Future，间歇性的等待结果返回。
        try {
            OTSFuture<ListTableResult> future = client.listTable();

            while (!future.isDone()) {
                System.out.println("Waiting for result of list table.");
                Thread.sleep(10); // 每隔10ms检查结果是否返回
            }

            ListTableResult result = future.get();
            System.out.println("\nList table by listTableWithFuture:");
            for (String tableName : result.getTableNames()) {
                System.out.println(tableName);
            }
        } catch (OTSException e) {
            e.printStackTrace();
        } catch (ClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void createTable(OTSClient client, String tableName)
            throws ServiceException, ClientException{
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyType.INTEGER);
        // 将该表的读写CU都设置为0
        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        CreateTableRequest request = new CreateTableRequest();
        request.setTableMeta(tableMeta);
        request.setReservedThroughput(capacityUnit);
        client.createTable(request);

        System.out.println("表已创建");
    }

    private static void deleteTable(OTSClient client, String tableName)
            throws ServiceException, ClientException{
        DeleteTableRequest request = new DeleteTableRequest();
        request.setTableName(tableName);
        client.deleteTable(request);

        System.out.println("表已删除");
    }
}
