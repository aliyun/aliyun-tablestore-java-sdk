package examples;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import com.alicloud.openservices.tablestore.DefaultTableStoreTimeseriesWriter;
import com.alicloud.openservices.tablestore.timeserieswriter.TimeseriesWriterResult;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesRowResult;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.enums.TSDispatchMode;
import com.alicloud.openservices.tablestore.timeserieswriter.enums.TSWriteMode;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TimeseriesWriterSample {
    private static String endpoint = "https://[instanceName].cn-hangzhou.ots.aliyuncs.com";
    private static String instanceName = "instanceName";
    private static String accessKeyId = "XXXXXXXXXX";
    private static String accessKeySecret = "XXXXXXXXXXXXXXXXXXXXXX";

    private static String tableName = "tableName";
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();

    public static void main(String[] args) {
        TimeseriesWriterSample sample = new TimeseriesWriterSample();

        /**
         * 使用Writer前确保表已存在
         * 经过测试，时序表创建30秒后向其中写入数据不会出现异常情况
         * writer会校验表的存在性
         * */
        sample.tryCreateTable();

        /**
         * 初始化建议使用
         * DefaultTableStoreTimeseriesWriter(
         *             String endpoint,                                                             // 实例域名
         *             ServiceCredentials credentials,                                              // 认证信息：含AK，也支持token
         *             String instanceName,                                                         // 实例名
         *             TimeseriesWriterConfig config,                                               // writer的配置
         *             TableStoreCallback<TimeseriesRow, TimeseriesRowResult> resultCallback)       // 行级别回调
         * */
        DefaultTableStoreTimeseriesWriter writer = sample.createTableStoreTimeseriesWriter();

        /**
         * Future使用： 批量写
         * */
        sample.writeTimeseriesRowWithFuture(writer);

        System.out.println("Count by TablestoreCallback: failedRow=" + failedRows.get() + ", succeedRow=" + succeedRows.get());
        System.out.println("Count by WriterStatics: " + writer.getTimeseriesWriterStatistics());

        /**
         * 用户需要主动关闭Writer，内部等候所有队列数据写入后，自动关闭client与内部的线程池
         * */
        writer.close();
    }

    private void tryCreateTable() {
        TimeseriesClient ots = new TimeseriesClient(endpoint, accessKeyId, accessKeySecret, instanceName);

//         删除表
        try {
            ots.deleteTimeseriesTable(new DeleteTimeseriesTableRequest(tableName));
        } catch (Exception e) {
        }


        TimeseriesTableMeta timeseriesTableMeta = new TimeseriesTableMeta(tableName);
        int timeToLive = -1;
        timeseriesTableMeta.setTimeseriesTableOptions(new TimeseriesTableOptions(timeToLive));
        CreateTimeseriesTableRequest request = new CreateTimeseriesTableRequest(timeseriesTableMeta);

        try {
            CreateTimeseriesTableResponse res = ots.createTimeseriesTable(request);
            System.out.println("waiting for creating time series table ......");
            TimeUnit.SECONDS.sleep(30);
        } catch (Exception e) {
            throw new ClientException(e);
        } finally {
            ots.shutdown();
        }
    }

    private DefaultTableStoreTimeseriesWriter createTableStoreTimeseriesWriter() {

        /**
         * 构造writer配置
         */
        TimeseriesWriterConfig config = new TimeseriesWriterConfig();
        config.setWriteMode(TSWriteMode.PARALLEL);                         // 并行写（每个桶内并行写）
        config.setDispatchMode(TSDispatchMode.HASH_PRIMARY_KEY);            // 基于主键哈希值做分桶，保证同主键落在一个桶内，有序写
        config.setBucketCount(4);                                          // 分桶数，提升串行写并发，未达机器瓶颈时与写入速率正相关
        config.setCallbackThreadCount(16);                                // 设置Writer内部Callback运行的线程池线程
        /**
         * 用户自定义的行级别callback
         * 该示例通过成功、失败计数，简单展示回调能力
         * */
        TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> resultCallback = new TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult>() {
            @Override
            public void onCompleted(TimeseriesTableRow rowChange, TimeseriesRowResult cc) {
                succeedRows.incrementAndGet();
            }

            @Override
            public void onFailed(TimeseriesTableRow rowChange, Exception ex) {
                failedRows.incrementAndGet();
            }
        };

        ServiceCredentials credentials = new DefaultCredentials(accessKeyId, accessKeySecret);

        /**
         * 推荐使用内部构建的线程池与Client，方便用户使用，隔离初始化、释放的逻辑
         * */
        DefaultTableStoreTimeseriesWriter writer = new DefaultTableStoreTimeseriesWriter(
                endpoint, credentials, instanceName, config, resultCallback);

        return writer;
    }

    private void writeTimeseriesRowWithFuture(DefaultTableStoreTimeseriesWriter writer) {
        System.out.println("=========================================================[Start]");
        System.out.print("Write Timeseries Row With Future, ");

        int rowsCount = 10000;
        int columnsCount = 10;
        AtomicLong rowIndex = new AtomicLong(-1);

        List<Future<TimeseriesWriterResult>> futures = new LinkedList<Future<TimeseriesWriterResult>>();
        List<TimeseriesTableRow> rows = new ArrayList<TimeseriesTableRow>();
        for (long index = rowIndex.incrementAndGet(); index < rowsCount; index = rowIndex.incrementAndGet()) {
            Map<String, String> tags = new HashMap<String, String>();
            tags.put("region", "hangzhou");
            tags.put("os", "Ubuntu16.04");
            // 通过measurementName、dataSource和tags构建TimeseriesKey。
            TimeseriesKey timeseriesKey = new TimeseriesKey("cpu", "host_" + index, tags);
            // 指定timeseriesKey和timeInUs创建timeseriesRow。
            TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000 + index);
            // 增加数据值（field）。
            for (int j = 0; j < columnsCount; j++) {
                row.addField("cpu_usage_" + j, ColumnValue.fromDouble(Math.random() * 100));
            }
            row.addField("index", ColumnValue.fromLong(index));
            TimeseriesTableRow rowInTable = new TimeseriesTableRow(row, tableName);
            rows.add(rowInTable);
            if (Math.random() > 0.9995 || index == rowsCount - 1) {
                Future<TimeseriesWriterResult> future = writer.addTimeseriesRowChangeWithFuture(rows);
                futures.add(future);
                rows.clear();
            }
        }
        System.out.println("Write thread finished.");
        writer.flush();
        printFutureResult(futures);
        System.out.println("=========================================================[Finish]");

    }

    private void printFutureResult(List<Future<TimeseriesWriterResult>> futures) {
        int totalRow = 0;
        System.out.println("time series writer results as follow：");
        for (int index = 0; index < futures.size(); index++) {
            try {
                TimeseriesWriterResult result = futures.get(index).get();
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
