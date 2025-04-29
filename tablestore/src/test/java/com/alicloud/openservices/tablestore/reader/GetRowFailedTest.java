package com.alicloud.openservices.tablestore.reader;

import java.util.ArrayList;
import java.util.List;
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

/**
 * Test case
 * testPartitionFailedRead: When BatchGetRow encounters PartitionFailed while reading data, the reader layer will not retry.
 * testTableStoreExceptionFailedRead: When BatchGetRow encounters a TableStoreException while reading data, it will be split into single-row requests for GetRow retries; if GetRow fails again, no further retries will be attempted.
 * testClientExceptionFailedRead: When BatchGetRow encounters a ClientException while reading data, the reader layer will not retry.
 */
public class GetRowFailedTest {
    private AsyncClientInterface ots;
    private AtomicLong succeedRows = new AtomicLong();
    private AtomicLong failedRows = new AtomicLong();
    private ExecutorService executorService;
    private TableStoreReader reader;
    @Test
    public void testPartitionFailedRead() throws InterruptedException {
        succeedRows.set(0);
        failedRows.set(0);

        List<Future<ReaderResult>> futures = new ArrayList<Future<ReaderResult>>();
        for (int i = 0; i < 10000; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i)).build();
            futures.add(reader.addPrimaryKeyWithFuture("partitionFailedTable", pk));
        }
        reader.flush();
        Thread.sleep(2000);

        assertEquals(5000, succeedRows.get());
        assertEquals(5000, failedRows.get());
        assertEquals(((DefaultTableStoreReader) reader).getStatistics().getTotalSingleRowRequestCount(), 0L);
    }

    @Test
    public void testTableStoreExceptionFailedRead() throws InterruptedException {
        succeedRows.set(0);
        failedRows.set(0);

        List<Future<ReaderResult>> futures = new ArrayList<Future<ReaderResult>>();
        for (int i = 0; i < 10000; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i)).build();
            futures.add(reader.addPrimaryKeyWithFuture("tableStoreExceptionFailedTable", pk));
        }
        reader.flush();
        Thread.sleep(2000);

        assertEquals(5000, succeedRows.get());
        assertEquals(5000, failedRows.get());
        assertEquals(((DefaultTableStoreReader) reader).getStatistics().totalSingleRowRequestCount.get(), 10000L);
    }

    @Test
    public void testClientExceptionFailedRead() throws InterruptedException {
        succeedRows.set(0);
        failedRows.set(0);

        List<Future<ReaderResult>> futures = new ArrayList<Future<ReaderResult>>();
        for (int i = 0; i < 10000; i++) {
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i)).build();
            futures.add(reader.addPrimaryKeyWithFuture("clientExceptionFailedTable", pk));
        }
        reader.flush();
        Thread.sleep(2000);

        assertEquals(0, succeedRows.get());
        assertEquals(10000, failedRows.get());
        assertEquals(((DefaultTableStoreReader) reader).getStatistics().totalSingleRowRequestCount.get(), 0L);
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
    }
}
