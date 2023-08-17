package com.alicloud.openservices.tablestore.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.DefaultTableStoreReader;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreReader;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RowQueryCriteria;
import com.alicloud.openservices.tablestore.reader.mock.MockReaderAsyncClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 测试case
 * testSetCallback:在读取过程中更换callback，功能正常
 * testSetRowQueryCriteria:在读取过程中更换rowQueryCriteria，功能正常
 */
public class TestSwitchReaderSetting {
    private final String tableName = "normalTable";
    private AsyncClientInterface ots;
    private AtomicLong succeedRows1 = new AtomicLong();
    private AtomicLong failedRows1 = new AtomicLong();
    private AtomicLong succeedRows2 = new AtomicLong();
    private AtomicLong failedRows2 = new AtomicLong();
    private ExecutorService executorService;
    private TableStoreReader reader;

    @Test
    public void testSetCallback() throws InterruptedException {
        succeedRows1.set(0);
        failedRows1.set(0);
        succeedRows2.set(0);
        failedRows2.set(0);
        // no callback
        List<Future<ReaderResult>> futures = new ArrayList<Future<ReaderResult>>();
        for (int i = 0; i < 1000; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i)).build();
            futures.add(reader.addPrimaryKeyWithFuture(tableName, pk));
        }
        reader.flush();
        Thread.sleep(500);

        assertEquals(succeedRows1.get(), 0);
        assertEquals(failedRows1.get(), 0);
        assertEquals(succeedRows2.get(), 0);
        assertEquals(failedRows2.get(), 0);
        // set callback1
        TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback1 = new TableStoreCallback<PrimaryKeyWithTable, RowReadResult>() {
            @Override
            public void onCompleted(PrimaryKeyWithTable req, RowReadResult res) {
                succeedRows1.incrementAndGet();
            }

            @Override
            public void onFailed(PrimaryKeyWithTable req, Exception ex) {
                failedRows1.incrementAndGet();
            }
        };
        reader.setCallback(callback1);

        for (int i = 0; i < 1100; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i)).build();
            futures.add(reader.addPrimaryKeyWithFuture(tableName, pk));
        }
        reader.flush();
        Thread.sleep(500);

        assertEquals(succeedRows1.get(), 1100);
        assertEquals(failedRows1.get(), 0);
        assertEquals(succeedRows2.get(), 0);
        assertEquals(failedRows2.get(), 0);
        // set callback2
        TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback2 = new TableStoreCallback<PrimaryKeyWithTable, RowReadResult>() {
            @Override
            public void onCompleted(PrimaryKeyWithTable req, RowReadResult res) {
                succeedRows2.incrementAndGet();
            }

            @Override
            public void onFailed(PrimaryKeyWithTable req, Exception ex) {
                failedRows2.incrementAndGet();
            }
        };
        reader.setCallback(callback2);

        for (int i = 0; i < 1200; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i)).build();
            futures.add(reader.addPrimaryKeyWithFuture(tableName, pk));
        }
        reader.flush();
        Thread.sleep(500);

        assertEquals(succeedRows1.get(), 1100);
        assertEquals(failedRows1.get(), 0);
        assertEquals(succeedRows2.get(), 1200);
        assertEquals(failedRows2.get(), 0);
    }

    @Test
    public void testSetRowQueryCriteria() throws InterruptedException, ExecutionException {
        // 未设置RowQueryCriteria， 默认读取所有行
        List<Future<ReaderResult>> futures = new ArrayList<Future<ReaderResult>>();
        for (int i = 0; i < 1000; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i)).build();
            futures.add(reader.addPrimaryKeyWithFuture(tableName, pk));
        }
        reader.flush();
        Thread.sleep(500);
        for (Future<ReaderResult> readerResult : futures
        ) {
            RowReadResult result = readerResult.get().getSucceedRows().get(0);
            assertEquals(result.getRowResult().getColumns().length, 10);
            assertEquals(result.getRowResult().getColumns()[0].getName(), "col_0");
            assertEquals(result.getRowResult().getColumns()[1].getName(), "col_1");
            assertEquals(result.getRowResult().getColumns()[2].getName(), "col_2");
            assertEquals(result.getRowResult().getColumns()[3].getName(), "col_3");
            assertEquals(result.getRowResult().getColumns()[4].getName(), "col_4");
            assertEquals(result.getRowResult().getColumns()[5].getName(), "col_5");
            assertEquals(result.getRowResult().getColumns()[6].getName(), "col_6");
            assertEquals(result.getRowResult().getColumns()[7].getName(), "col_7");
            assertEquals(result.getRowResult().getColumns()[8].getName(), "col_8");
            assertEquals(result.getRowResult().getColumns()[9].getName(), "col_9");
        }
        // 设置RowQueryCriteria，读取指定行
        RowQueryCriteria criteria = new RowQueryCriteria(tableName);
        criteria.addColumnsToGet("col_0");
        criteria.addColumnsToGet("col_1");
        criteria.addColumnsToGet("col_5");
        reader.setRowQueryCriteria(criteria);
        List<Future<ReaderResult>> futures1 = new ArrayList<Future<ReaderResult>>();
        for (int i = 0; i < 1000; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i)).build();
            futures1.add(reader.addPrimaryKeyWithFuture(tableName, pk));
        }
        reader.flush();
        Thread.sleep(500);
        for (Future<ReaderResult> readerResult : futures1
        ) {
            RowReadResult result = readerResult.get().getSucceedRows().get(0);
            assertEquals(result.getRowResult().getColumns().length, 3);
            assertEquals(result.getRowResult().getLatestColumn("col_0").getValue().asString(), "value_of_col_0");
            assertEquals(result.getRowResult().getLatestColumn("col_1").getValue().asString(), "value_of_col_1");
            assertEquals(result.getRowResult().getLatestColumn("col_5").getValue().asString(), "value_of_col_5");
        }
    }

    @Before
    public void setUp() throws Exception {
        ots = new MockReaderAsyncClient();

        TableStoreReaderConfig config = new TableStoreReaderConfig();
        config.setFlushInterval(60 * 60 * 1000);
        config.setBucketCount(1);
        config.setCheckTableMeta(false);

        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "reader-" + counter.getAndIncrement());
            }
        };
        executorService = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(1024), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

        reader = new DefaultTableStoreReader(ots, config, executorService, null);
    }

    @After
    public void close() {
        try {
            reader.close();
        } catch (ClientException e) {
            assertEquals(e.getMessage(), "The reader has already been closed.");
        }
        ots.shutdown();
        executorService.shutdown();
    }
}
