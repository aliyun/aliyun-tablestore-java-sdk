package examples;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.RowWriteResult;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.writer.WriterResult;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import com.alicloud.openservices.tablestore.writer.enums.DispatchMode;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.commons.codec.digest.DigestUtils.md5Hex;


public class WriterSample {

    private static String endpoint = "https://[instanceName].cn-hangzhou.ots.aliyuncs.com";
    private static String instanceName = "instanceName";
    private static String accessKeyId = "XXXXXXXXXX";
    private static String accessKeySecret = "XXXXXXXXXXXXXXXXXXXXXX";
    private static String tableName = "tableName";

    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();

    public static void main(String[] args) {
        WriterSample sample = new WriterSample();

        /**
         * 使用Writer前确保表已存在
         * 1、writer会校验表的存在性
         * 2、校验写入数据是否与表的字段、类型一致
         * */
        sample.tryCreateTable();

        /**
         * 初始化建议使用
         * DefaultTableStoreWriter(
         *      String endpoint,                                                   // 实例域名
         *      ServiceCredentials credentials,                                    // 认证信息：含AK，也支持token
         *      String instanceName,                                               // 实例名
         *      String tableName,                                                  // 表名：一个writer仅针对一个表
         *      WriterConfig config,                                               // writer的配置
         *      TableStoreCallback<RowChange, RowWriteResult> resultCallback       // 行级别回调
         * )
         * */
        TableStoreWriter writer = sample.createTablesStoreWriter();

        /**
         * Future使用： 批量写
         * */
        sample.writeRowListWithFuture(writer);

        /**
         * Future使用： 单行写
         * */
        sample.writeSingleRowWithFuture(writer);

        System.out.println("Count by TablestoreCallback: failedRow=" + failedRows.get() + ", succeedRow=" + succeedRows.get());
        System.out.println("Count by WriterStatics: " + writer.getWriterStatistics());

        /**
         * 用户需要主动关闭Writer，内部等候所有队列数据写入后，自动关闭client与内部的线程池
         * */
        writer.close();
    }


    private TableStoreWriter createTablesStoreWriter() {

        WriterConfig config = new WriterConfig();
        config.setWriteMode(WriteMode.SEQUENTIAL);                      // 串行写（每个桶内串行写）
        config.setBatchRequestType(BatchRequestType.BULK_IMPORT);       // 底层构建BulkImportRequest做批量写
        config.setDispatchMode(DispatchMode.HASH_PRIMARY_KEY);          // 基于主键哈希值做分桶，保证同主键落在一个桶内，有序写
        config.setBucketCount(100);                                     // 分桶数，提升串行写并发，未达机器瓶颈时与写入速率正相关
        config.setCallbackThreadCount(16);                              // 设置Writer内部Callback运行的线程池线程数
        config.setAllowDuplicatedRowInBatchRequest(false);              // 底层构建的批量请求内，不允许有重复行（主要针对二级索引，如果含有忽略用户设置，false覆盖）

        /**
         * 用户自定义的行级别callback
         * 该示例通过成功、失败计数，简单展示回调能力
         * */
        TableStoreCallback<RowChange, RowWriteResult> resultCallback = new TableStoreCallback<RowChange, RowWriteResult>() {
            @Override
            public void onCompleted(RowChange rowChange, RowWriteResult cc) {
                succeedRows.incrementAndGet();
            }

            @Override
            public void onFailed(RowChange rowChange, Exception ex) {
                failedRows.incrementAndGet();
            }
        };

        ServiceCredentials credentials = new DefaultCredentials(accessKeyId, accessKeySecret);


        /**
         * 推荐使用内部构建的线程池与Client，方便用户使用，隔离初始化、释放的逻辑
         * */
        DefaultTableStoreWriter writer = new DefaultTableStoreWriter(
                endpoint, credentials, instanceName, tableName, config, resultCallback);

        return writer;
    }


    private void tryCreateTable() {
        SyncClient ots = new SyncClient(endpoint, accessKeyId, accessKeySecret, instanceName);

        try {
            ots.deleteTable(new DeleteTableRequest(tableName));
        } catch (Exception e) {
        }

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk_0", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk_1", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk_2", PrimaryKeyType.INTEGER);
        TableOptions tableOptions = new TableOptions(-1, 1);
        CreateTableRequest request = new CreateTableRequest(
                tableMeta, tableOptions, new ReservedThroughput(new CapacityUnit(0, 0)));

        try {
            CreateTableResponse res = ots.createTable(request);
        } catch (Exception e) {
            throw new ClientException(e);
        } finally {
            ots.shutdown();
        }
    }


    public void writeSingleRowWithFuture(TableStoreWriter writer) {
        System.out.println("=========================================================[Start]");
        System.out.println("Write Single Row With Future");
        int rowsCount = 20;
        int columnsCount = 10;
        String strValue = "1234567890";
        AtomicLong rowIndex = new AtomicLong(-1);

        List<Future<WriterResult>> futures = new LinkedList<Future<WriterResult>>();
        for (long index = rowIndex.incrementAndGet(); index < rowsCount; index = rowIndex.incrementAndGet()) {

            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(md5Hex(index + "")))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("pk" + index))
                    .addPrimaryKeyColumn("pk_2", PrimaryKeyValue.fromLong(index % 5))
                    .build();

            RowUpdateChange rowChange = new RowUpdateChange(tableName, pk);
            for (int j = 0; j < columnsCount; j++) {
                rowChange.put("column_" + j, ColumnValue.fromString(strValue));
            }
            rowChange.put("index", ColumnValue.fromLong(index));
            Future<WriterResult> future = writer.addRowChangeWithFuture(rowChange);
            futures.add(future);
        }

        System.out.println("Write thread finished.");
        writer.flush();

        printFutureResult(futures);

        System.out.println("=========================================================[Finish]");
    }


    public void writeRowListWithFuture(TableStoreWriter writer) {
        System.out.println("=========================================================[Start]");
        System.out.println("Write Row List With Future");

        int rowsCount = 10000;
        int columnsCount = 10;
        String strValue = "1234567890";
        AtomicLong rowIndex = new AtomicLong(-1);

        List<Future<WriterResult>> futures = new LinkedList<Future<WriterResult>>();
        List<RowChange> rowChanges = new LinkedList<RowChange>();
        for (long index = rowIndex.incrementAndGet(); index < rowsCount; index = rowIndex.incrementAndGet()) {

            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk_0", PrimaryKeyValue.fromString(md5Hex(index + "")))
                    .addPrimaryKeyColumn("pk_1", PrimaryKeyValue.fromString("pk" + index))
                    .addPrimaryKeyColumn("pk_2", PrimaryKeyValue.fromLong(index % 5))
                    .build();

            RowUpdateChange rowChange = new RowUpdateChange(tableName, pk);
            for (int j = 0; j < columnsCount; j++) {
                rowChange.put("column_" + j, ColumnValue.fromString(strValue));
            }
            rowChange.put("index", ColumnValue.fromLong(index));
            rowChanges.add(rowChange);
            if (Math.random() > 0.995 || index == rowsCount - 1) {
                Future<WriterResult> future = writer.addRowChangeWithFuture(rowChanges);
                futures.add(future);
                rowChanges.clear();
            }
        }

        System.out.println("Write thread finished.");
        writer.flush();
        printFutureResult(futures);
        System.out.println("=========================================================[Finish]");
    }

    private void printFutureResult(List<Future<WriterResult>> futures) {
        int totalRow = 0;

        for (int index = 0; index < futures.size(); index++) {
            try {
                WriterResult result = futures.get(index).get();
                totalRow += result.getTotalCount();
                System.out.println(String.format(
                        "Future[%d] finished:\tfailed: %d\tsucceed: %d\tfutureBatch: %d\ttotalFinished: %d",
                        index, result.getFailedRows().size(), result.getSucceedRows().size(),
                        result.getTotalCount(), totalRow));

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
