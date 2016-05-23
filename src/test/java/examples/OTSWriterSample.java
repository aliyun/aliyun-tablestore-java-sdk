package examples;

import com.aliyun.openservices.ots.*;
import com.aliyun.openservices.ots.internal.OTSAlwaysRetryStrategy;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.internal.OTSDefaultRetryStrategy;
import com.aliyun.openservices.ots.internal.writer.WriterConfig;
import com.aliyun.openservices.ots.model.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class OTSWriterSample {
    private static final String COLUMN_GID_NAME = "gid";
    private static final String COLUMN_UID_NAME = "uid";
    final private static String endPoint = "";
    final private static String accessId = "";
    final private static String accessKey = "";
    final private static String instanceName = "";


    public static void main(String args[]) {

        OTSClient client = new OTSClient(endPoint, accessId, accessKey,
                instanceName);
        final String tableName = "sampleTable";

        try {
            // 创建表
            createTable(client, tableName);

            writeRow(tableName);

            scanTable(client, tableName);

            deleteTable(client, tableName);

        } catch (ServiceException e) {
            System.err.println("操作失败，详情：" + e.getMessage());
            // 可以根据错误代码做出处理， OTS的ErrorCode定义在OTSErrorCode中。
            if (OTSErrorCode.QUOTA_EXHAUSTED.equals(e.getErrorCode())) {
                System.err.println("超出存储配额。");
            }
            // Request ID可以用于有问题时联系客服诊断异常。
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            // 可能是网络不好或者是返回结果有问题
            System.err.println("请求失败，详情：" + e.getMessage());
        } catch (InterruptedException e) {
            System.err.print(e);
        }
        client.shutdown();
    }

    private static void scanTable(OTSClient client, String tableName) {
        RangeIteratorParameter param = new RangeIteratorParameter(tableName);
        RowPrimaryKey startKey = new RowPrimaryKey();
        startKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MIN);
        startKey.addPrimaryKeyColumn("gid", PrimaryKeyValue.INF_MIN);

        RowPrimaryKey endKey = new RowPrimaryKey();
        endKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MAX);
        endKey.addPrimaryKeyColumn("gid", PrimaryKeyValue.INF_MAX);

        param.setInclusiveStartPrimaryKey(startKey);
        param.setExclusiveEndPrimaryKey(endKey);

        Iterator<Row> rowIter = client.createRangeIterator(param);
        int totalCount = 0;
        while (rowIter.hasNext()) {
            rowIter.next();
            totalCount++;
        }

        System.out.println("TotalRows in table: " + totalCount);
    }

    private static void createTable(OTSClient client, String tableName)
            throws ServiceException, ClientException {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyType.INTEGER);
        // 将该表的读写CU都设置为100
        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        CreateTableRequest request = new CreateTableRequest();
        request.setTableMeta(tableMeta);
        request.setReservedThroughput(capacityUnit);
        client.createTable(request);

        System.out.println("表已创建");
    }

    private static void deleteTable(OTSClient client, String tableName)
            throws ServiceException, ClientException {
        DeleteTableRequest request = new DeleteTableRequest();
        request.setTableName(tableName);
        client.deleteTable(request);

        System.out.println("表已删除");
    }

    private static void writeRow(String tableName) throws InterruptedException {
        ClientConfiguration cc = new ClientConfiguration();
        OTSServiceConfiguration osc = new OTSServiceConfiguration();
        osc.setRetryStrategy(new OTSDefaultRetryStrategy()); // 可定制重试策略，若需要保证数据写入成功率，可采用更激进的重试策略
        OTSClientAsync asyncClient = new OTSClientAsync(endPoint, accessId, accessKey, instanceName, cc, osc);

        // 初始化
        WriterConfig config = new WriterConfig();
        config.setMaxBatchSize(1024 * 1024); // 配置一次批量导入请求的大小限制，默认是1MB
        config.setMaxColumnsCount(128); // 配置一行的列数的上限，默认128列
        config.setBufferSize(1024); // 配置内存中最多缓冲的数据行数，默认1024行，必须是2的指数倍
        config.setMaxBatchRowsCount(100); // 配置一次批量导入的行数上限，默认100
        config.setConcurrency(10); // 配置最大并发数，默认10
        config.setMaxAttrColumnSize(64 * 1024); // 配置属性列的值大小上限，默认是64KB
        config.setMaxPKColumnSize(1024); // 配置主键列的值大小上限，默认1KB
        config.setFlushInterval(10000); // 配置缓冲区flush的时间间隔，默认10s

        // 配置一个callback，OTSWriter通过该callback反馈哪些导入成功，哪些行导入失败，该callback只简单的统计写入成功和失败的行数。

        AtomicLong succeedCount = new AtomicLong();
        AtomicLong failedCount = new AtomicLong();
        OTSCallback<RowChange, ConsumedCapacity> callback = new SampleCallback(succeedCount, failedCount);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        OTSWriter otsWriter = new DefaultOTSWriter(asyncClient, tableName, config, callback, executor);

        // 起多个线程，并发的导入数据
        int threadCount = 10;
        int rowsCount = 10000;
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < threadCount; i++) {
            WriteRow writeRow = new WriteRow(i, tableName, otsWriter, rowsCount);
            threads.add(new Thread(writeRow));
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // 所有压力线程完成后，等待OTSWriter将数据导入完毕
        otsWriter.flush();

        // 所有数据导入完毕后，需要显式的将OTSWriter给close掉
        otsWriter.close();

        // 最终关闭ots client
        asyncClient.shutdown();

        // 关闭executor thread pool
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        System.out.println("成功导入行数: " + succeedCount);
        System.out.println("失败导入行数: " + failedCount);
    }

    private static class WriteRow implements Runnable {
        private int id;
        private String tableName;
        private OTSWriter writer;
        private int rowsCount;

        public WriteRow(int id, String tableName, OTSWriter writer, int rowsCount) {
            this.id = id;
            this.tableName = tableName;
            this.writer = writer;
            this.rowsCount = rowsCount;
        }

        @Override
        public void run() {
            int start = id * rowsCount;
            for (int i = 0; i < rowsCount; i++) {
                RowPrimaryKey primaryKey = new RowPrimaryKey();
                primaryKey.addPrimaryKeyColumn("gid", PrimaryKeyValue.fromLong(start + i));
                primaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.fromLong(start + i));

                RowPutChange rowChange = new RowPutChange(tableName);
                rowChange.setPrimaryKey(primaryKey);
                rowChange.addAttributeColumn("col1", ColumnValue.fromBoolean(true));
                rowChange.addAttributeColumn("col2", ColumnValue.fromLong(10));
                rowChange.addAttributeColumn("col3", ColumnValue.fromString("Hello world."));

                writer.addRowChange(rowChange);
            }
        }
    }

    private static class SampleCallback implements OTSCallback<RowChange, ConsumedCapacity> {
        private AtomicLong succeedCount;
        private AtomicLong failedCount;

        public SampleCallback(AtomicLong succeedCount, AtomicLong failedCount) {
            this.succeedCount = succeedCount;
            this.failedCount = failedCount;
        }

        @Override
        public void onCompleted(OTSContext<RowChange, ConsumedCapacity> otsContext) {
            succeedCount.incrementAndGet();
        }

        @Override
        public void onFailed(OTSContext<RowChange, ConsumedCapacity> otsContext, OTSException ex) {
            ex.printStackTrace();
            failedCount.incrementAndGet();
        }

        @Override
        public void onFailed(OTSContext<RowChange, ConsumedCapacity> otsContext, ClientException ex) {
            ex.printStackTrace();
            failedCount.incrementAndGet();
        }
    }
}
