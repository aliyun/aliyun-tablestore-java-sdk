package com.alicloud.openservices.tablestore.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
import com.alicloud.openservices.tablestore.reader.mock.MockReaderAsyncClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 测试case
 * testNormalRead:单线程使用reader，读取normalTable中的10000条数据，读取全部成功
 * testParallelRead:4个线程并发使用reader，测试读取normalTable中的100000条数据，读取全部成功
 * testCloseReader:向内存添加数据后，直接关闭reader，内存中的数据同样会被查询
 */
public class ReaderTest {
    private final String tableName = "normalTable";
    private AsyncClientInterface ots;
    private AtomicLong succeedRows = new AtomicLong();
    private AtomicLong failedRows = new AtomicLong();
    private ExecutorService executorService;
    private TableStoreReader reader;

    @Test
    public void testNormalRead() throws InterruptedException, ExecutionException {
        succeedRows.set(0);
        failedRows.set(0);

        List<Future<ReaderResult>> futures = new ArrayList<Future<ReaderResult>>();
        for (int i = 0; i < 10000; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i)).build();
            futures.add(reader.addPrimaryKeyWithFuture(tableName, pk));
        }
        reader.flush();
        Thread.sleep(500);

        assertEquals(10000, succeedRows.get());
        assertEquals(0, failedRows.get());
        for (Future<ReaderResult> readerResult : futures
        ) {
            assertEquals(readerResult.get().getSucceedRows().size(), 1);
            assertEquals(readerResult.get().getFailedRows().size(), 0);

            RowReadResult result = readerResult.get().getSucceedRows().get(0);
            assertTrue(result.isSucceed());
            assertEquals(result.getTableName(), tableName);
            assertEquals(result.getPrimaryKey(), result.getRowResult().getPrimaryKey());
            assertEquals(result.getRowResult().getColumns().length, 10);
        }
    }

    @Test
    public void testParallelRead() {
        succeedRows.set(0);
        failedRows.set(0);

        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ex-" + counter.getAndIncrement());
            }
        };
        final ExecutorService readerExecutor = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(1024), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

        final AtomicLong counts = new AtomicLong();
        final CountDownLatch countDownLatch = new CountDownLatch(4);
        for (int i = 0; i < 4; i++) {
            final int finalI = i;

            readerExecutor.submit(new Runnable() {
                public void run() {

                    List<Future<ReaderResult>> futures = new ArrayList<Future<ReaderResult>>();
                    int index = 0;
                    for (; counts.getAndIncrement() < 100000; ) {
                        index += 1;
                        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(finalI * 100000 + index)).build();
                        futures.add(reader.addPrimaryKeyWithFuture(tableName, pk));
                    }
                    reader.send();

                    System.out.println(String.format("Threads %s read %s rows.", finalI, futures.size()));
                    for (Future<ReaderResult> readerResult : futures
                    ) {
                        ReaderResult result;
                        try {
                            result = readerResult.get();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                        assertEquals(result.getSucceedRows().size(), 1);
                        assertEquals(result.getFailedRows().size(), 0);
                        RowReadResult rowReadResult = result.getSucceedRows().get(0);
                        assertTrue(rowReadResult.isSucceed());
                        assertEquals(rowReadResult.getTableName(), tableName);
                        assertEquals(rowReadResult.getPrimaryKey(), rowReadResult.getRowResult().getPrimaryKey());
                        assertEquals(rowReadResult.getRowResult().getColumns().length, 10);
                    }
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertEquals(100000, succeedRows.get());
        assertEquals(0, failedRows.get());
        readerExecutor.shutdown();
    }

    @Test
    public void testReaderClose() throws InterruptedException {
        reader.flush();
        succeedRows.set(0);
        failedRows.set(0);

        List<Future<ReaderResult>> futures = new ArrayList<Future<ReaderResult>>();
        for (int i = 0; i < 999; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i)).build();
            futures.add(reader.addPrimaryKeyWithFuture(tableName, pk));
        }
        Thread.sleep(500);

        // 内存中会剩余99条数据
        assertEquals(900, succeedRows.get());
        assertEquals(0, failedRows.get());

        reader.close();
        Thread.sleep(500);
        assertEquals(999, succeedRows.get());
        assertEquals(0, failedRows.get());
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

        TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback = new TableStoreCallback<PrimaryKeyWithTable, RowReadResult>() {
            @Override
            public void onCompleted(PrimaryKeyWithTable req, RowReadResult res) {
                succeedRows.incrementAndGet();
            }

            @Override
            public void onFailed(PrimaryKeyWithTable req, Exception ex) {
                failedRows.incrementAndGet();
            }
        };

        reader = new DefaultTableStoreReader(ots, config, executorService, callback);

        Thread.sleep(1000);
    }

    @After
    public void close() {
        try {
            reader.close();
        } catch (ClientException e) {
            assertEquals(e.getMessage(), "The reader has already been closed.");
        }
        ots.shutdown();
        executorService.shutdownNow();
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
