package com.alicloud.openservices.tablestore.timeserieswriter;

import com.alicloud.openservices.tablestore.AsyncTimeseriesClient;
import com.alicloud.openservices.tablestore.DefaultTableStoreTimeseriesWriter;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;
import com.alicloud.openservices.tablestore.model.TimeseriesTableOptions;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesRowResult;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class TestTimeseriesWriterMutiTables {
    private static final String tableName = "WriterTest";
    private static ServiceSettings serviceSettings = ServiceSettings.load();
    private static AtomicLong succeedRows = new AtomicLong();
    private static AtomicLong failedRows = new AtomicLong();
    private static long rowsCount = 100000;
    private static long columnsCount = 10;
    private static int concurrency = 100;
    private static int queueSize = 4096;
    private static int tablesCount = 4;
    private AsyncTimeseriesClient ots;
    final String strValue = "0123456789";
    private static AtomicLong retryCount = new AtomicLong();

    public void createTimeseriesTable(AsyncTimeseriesClient client, String tableName) throws Exception {
        TimeseriesTableMeta timeseriesTableMeta = new TimeseriesTableMeta(tableName);
        int timeToLive = -1;
        timeseriesTableMeta.setTimeseriesTableOptions(new TimeseriesTableOptions(timeToLive));
        CreateTimeseriesTableRequest request = new CreateTimeseriesTableRequest(timeseriesTableMeta);
        Future<CreateTimeseriesTableResponse> future = client.createTimeseriesTable(request, null);
        future.get();
    }

    @Before
    public void setUp() throws Exception {
        succeedRows.getAndSet(0);
        failedRows.getAndSet(0);

        ots = new AsyncTimeseriesClient(serviceSettings.getOTSEndpoint(),
                serviceSettings.getOTSAccessKeyId(), serviceSettings.getOTSAccessKeySecret(),
                serviceSettings.getOTSInstanceName());

        for (int i = 0; i < tablesCount; i++) {
            try {
                DeleteTimeseriesTableRequest request = new DeleteTimeseriesTableRequest(tableName+i);
                Future<DeleteTimeseriesTableResponse> future = ots.deleteTimeseriesTable(request, null);
                future.get();

            } catch (Exception e) {
                // pass
            }
            createTimeseriesTable(ots, tableName+i);
        }


        System.out.println("waiting...");
        Thread.sleep(30000);
        System.out.println("stop waiting");

    }

    @After
    public void after() {
        ots.shutdown();
    }

    @Test
    public void testWriteMutiTable() throws Exception {
        final TimeseriesWriterConfig config = new TimeseriesWriterConfig();
        config.setConcurrency(concurrency);
        config.setBufferSize(queueSize);
        config.setFlushInterval(1000000);
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        DefaultTableStoreTimeseriesWriter writer = new DefaultTableStoreTimeseriesWriter(ots, config, null, executor);

        final AtomicLong gotRowCount = new AtomicLong(0);
        writer.setResultCallback(new TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult>() {
            @Override
            public void onCompleted(TimeseriesTableRow rowChange, TimeseriesRowResult cc) {
                if (cc.isSuccess()) {
                    gotRowCount.incrementAndGet();
                }
                succeedRows.incrementAndGet();
            }

            @Override
            public void onFailed(TimeseriesTableRow rowChange, Exception ex) {
                failedRows.incrementAndGet();
            }
        });

        try {
            for (int i = 0; i < 100; i++) {
                Map<String, String> tags = new HashMap<String, String>();
                tags.put("region", "hangzhou");
                tags.put("os", "Ubuntu16.04");
                TimeseriesKey timeseriesKey = new TimeseriesKey("cpu" + i, "host_" + i, tags);
                TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000 + i);
                for (int j = 0; j < columnsCount; j++) {
                    row.addField("column_" + j, ColumnValue.fromString(strValue));
                }

                writer.addTimeseriesRowChange(new TimeseriesTableRow(row, tableName + i % tablesCount));
            }

            writer.flush();

        } finally {
            writer.close();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        System.out.println(writer.getTimeseriesWriterStatistics());

        assertEquals(succeedRows.get(), 100);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalSucceedRowsCount(), 100);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalFailedRowsCount(), 0);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalRequestCount(), 4 * tablesCount);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalRowsCount(), 100);
        assertEquals(writer.getTimeseriesWriterStatistics().getTotalSingleRowRequestCount(), 0);
        assertEquals(gotRowCount.get(), 100);
    }

}
