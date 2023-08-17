package examples;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.DefaultTableStoreReader;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreReader;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RowQueryCriteria;
import com.alicloud.openservices.tablestore.reader.PrimaryKeyWithTable;
import com.alicloud.openservices.tablestore.reader.ReaderResult;
import com.alicloud.openservices.tablestore.reader.RowReadResult;
import com.alicloud.openservices.tablestore.reader.TableStoreReaderConfig;

public class ReaderSample {
    private static final String endpoint = "https://xxx.ots.aliyuncs.com";
    private static final String accessKeyId = "";
    private static final String accessKeySecret = "";
    private static final String instanceName = "";
    private static AsyncClientInterface client;
    private static ExecutorService executorService;
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        TableStoreReader reader = createReader();

        // 设置表'testTableStoreReaderTable'的查询列为col1、最大查询版本为10
        // 若查询的表未设置RowQueryCriteria，则默认查询所有属性列，查询最大查询版本为1
        RowQueryCriteria criteria = new RowQueryCriteria("testTableStoreReaderTable");
        criteria.addColumnsToGet("col1");
        criteria.setMaxVersions(10);
        reader.setRowQueryCriteria(criteria);

        // 向内存添加一列要查询的数据
        PrimaryKey pk1 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(0))
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(0))
                .build();
        // 查询表testTableStoreReaderTable中的pk1的属性列
        reader.addPrimaryKey("testTableStoreReaderTable", pk1);

        // 异步将内存中的数据发送出去
        reader.send();

        // 向内存添加一列要查询的数据，并获取查询结果Future
        PrimaryKey pk2 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(0))
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(0))
                .build();

        // 查询表anotherTable中的pk2的属性列
        Future<ReaderResult> readerResult = reader.addPrimaryKeyWithFuture("anotherTable", pk2);

        // 同步将内存中的数据发送出去
        reader.flush();

        // 等待回调函数处理完成
        Thread.sleep(1000);

        System.out.println("result:" + readerResult.get().toString());
        System.out.println("succeed rows count:" + succeedRows.get());
        System.out.println("failed rows count:" + failedRows.get());

        reader.close();
        client.shutdown();
        executorService.shutdown();
    }

    public static TableStoreReader createReader() {
        // 构造AsyncClient
        client = new AsyncClient(endpoint, accessKeyId, accessKeySecret, instanceName);

        // 构造reader配置类
        TableStoreReaderConfig config = new TableStoreReaderConfig();
        {
            // 以下参数均有默认值，可以不进行配置
            config.setCheckTableMeta(true);                 // 向reader添加要数据前，会先检查表的结构
            config.setMaxBatchRowsCount(100);               // 一次请求的最大请求行数，上限为100
            config.setDefaultMaxVersions(1);                // 默认情况下，获取的columns最大版本数
            config.setConcurrency(16);                      // 发送请求的总并发数
            config.setBucketCount(4);                       // 内存分桶数
            config.setFlushInterval(10000);                 // 将缓存数据全部发送的时间间隔
            config.setLogInterval(10000);                   // 日志记录reader状态的时间间隔
        }

        // 构造用于发送请求的executor
        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "reader-" + counter.getAndIncrement());
            }
        };
        executorService = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(1024), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

        // 构造reader的回调函数
        TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback = new TableStoreCallback<PrimaryKeyWithTable, RowReadResult>() {
            @Override
            public void onCompleted(PrimaryKeyWithTable req, RowReadResult res) {
                succeedRows.incrementAndGet();
                System.out.println(res.getRowResult());
            }

            @Override
            public void onFailed(PrimaryKeyWithTable req, Exception ex) {
                failedRows.incrementAndGet();
            }
        };
        TableStoreReader reader = new DefaultTableStoreReader(client, config, executorService, callback);
        return reader;
    }
}
