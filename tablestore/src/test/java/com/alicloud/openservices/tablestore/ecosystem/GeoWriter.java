package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.DefaultTableStoreWriter;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreWriter;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 *
 * This function needs to be run to construct the original dataset and build the multi-index before running testGetUnhandledOtsFilter/testSplitsGenerationForSearchIndex.
 */

public class GeoWriter {
    private static Logger logger = LoggerFactory.getLogger(GeoWriter.class);
    public static String endpoint = "";
    public static String AccessKey = "";
    public static String SECRET = "";
    public static String instanceName = "sxSparkSearch";
    public static String tableName = "geo_table";
    public static String indexName = "geo_table_index3";

    public static AsyncClient asyncClient = new AsyncClient(endpoint, AccessKey, SECRET, instanceName);
    public static AtomicLong succeedCount = new AtomicLong();
    public static AtomicLong failedCount = new AtomicLong();

    public static void main(String[] args) {
        try {
            writeData(200000000L);
        } finally {
            asyncClient.shutdown();
        }
    }

    public static void writeData(long total) {
        TableStoreWriter tablestoreWriter = getWriter();
        while (succeedCount.get() < total) {
            tablestoreWriter.addRowChange(generateData());
        }
        tablestoreWriter.flush();
        tablestoreWriter.close();
    }

    public static RowChange generateData() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(RandomHelper.randomString(30)))
                .build();
        RowPutChange rowChange = new RowPutChange(tableName);
        rowChange.setPrimaryKey(primaryKey);
        rowChange.addColumn("val_keyword1", ColumnValue.fromString(RandomHelper.randomString(3)));
        rowChange.addColumn("val_keyword2", ColumnValue.fromString(RandomHelper.randomString(RandomHelper.randomInt(10))));
        rowChange.addColumn("val_keyword3", ColumnValue.fromString(RandomHelper.randomString(RandomHelper.randomInt(50))));
        rowChange.addColumn("val_bool", ColumnValue.fromBoolean(RandomHelper.randomBool()));
        rowChange.addColumn("val_double", ColumnValue.fromDouble(RandomHelper.randomDouble(10000)));
        rowChange.addColumn("val_long1", ColumnValue.fromLong(RandomHelper.randomLong(100000000)));
        rowChange.addColumn("val_long2", ColumnValue.fromLong(RandomHelper.randomLong(10000)));
        rowChange.addColumn("val_text", ColumnValue.fromString(RandomHelper.randomText(RandomHelper.randomInt(15), 4)));
        rowChange.addColumn("val_geo", ColumnValue.fromString(RandomHelper.randomDouble(80) + "," + RandomHelper.randomDouble(80)));
        return rowChange;
    }

    public static TableStoreWriter getWriter() {
        WriterConfig config = new WriterConfig();
        config.setMaxBatchSize(4 * 1024 * 1024);
        config.setMaxColumnsCount(128);
        config.setBufferSize(1024);
        config.setMaxBatchRowsCount(100);
        config.setConcurrency(10);
        config.setMaxAttrColumnSize(2 * 1024 * 1024);
        config.setMaxPKColumnSize(1024);
        config.setFlushInterval(10000);

        TableStoreCallback<RowChange, ConsumedCapacity> callback = new TableStoreCallback<RowChange, ConsumedCapacity>() {
            @Override
            public void onCompleted(RowChange req, ConsumedCapacity res) {
                succeedCount.incrementAndGet();
            }

            @Override
            public void onFailed(RowChange req, Exception ex) {
                failedCount.incrementAndGet();
            }
        };
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("geotable-runner-%d").build();
        int size = 10;
        ExecutorService executor = new ThreadPoolExecutor(size, size, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), namedThreadFactory);
        return new DefaultTableStoreWriter(asyncClient, tableName, config, callback, executor);
    }
}

class RandomHelper {

    static final char[] ALPHABET = "0123456789_abcdefghijklmnopqrstuvwxyz".toCharArray();

    public static String randomString(int length) {
        char[] ret = new char[length];
        for (int i = 0; i < length; i++) {
            ret[i] = ALPHABET[(int) (System.nanoTime() % ALPHABET.length)];
        }
        return new String(ret);
    }

    public static String randomText(int length, int termLength) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            if (i != 0) {
                sb.append(" ");
            }
            sb.append(randomString(termLength));
        }
        return sb.toString();
    }

    public static long randomLong(long max) {
        return System.nanoTime() % max;
    }

    public static int randomInt(int max) {
        return Long.valueOf(System.nanoTime() % max).intValue();
    }

    public static boolean randomBool() {
        return System.nanoTime() % 2 == 0;
    }

    public static double randomDouble(double bound) {
        return new Random().nextDouble();
    }

}
