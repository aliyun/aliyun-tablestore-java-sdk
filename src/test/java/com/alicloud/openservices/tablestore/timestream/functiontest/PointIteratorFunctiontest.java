package com.alicloud.openservices.tablestore.timestream.functiontest;

import com.alicloud.openservices.tablestore.timestream.bench.Conf;
import com.alicloud.openservices.tablestore.timestream.bench.wrapper.PrimaryKeyWrapper;
import com.alicloud.openservices.tablestore.timestream.bench.wrapper.TableStoreWrapper;
import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.model.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PointIteratorFunctiontest {
    private static Logger logger = LoggerFactory.getLogger(PointIteratorFunctiontest.class);
    private static Conf conf;

    @BeforeClass
    public static void init() throws FileNotFoundException {
        logger.debug("load configuration");
        conf = Conf.newInstance("src/test/resources/test_conf.json");
    }

    @AfterClass
    public static void close() {

    }

    @Test
    public void testBasic() throws ExecutionException, InterruptedException {
        String table = "PointIteratorFunctiontest";

        AsyncClient client = new AsyncClient(conf.getEndpoint(), conf.getAccessId(), conf.getAccessKey(), conf.getInstance());
        TableStoreWrapper wrapper = TableStoreWrapper.instance(client, table);

        wrapper.deleteTable();

        wrapper.createTable()
                .addPrimaryKey(TableMetaGenerator.CN_PK0, PrimaryKeyType.STRING)
                .addPrimaryKey(TableMetaGenerator.CN_PK1, PrimaryKeyType.STRING)
                .addPrimaryKey(TableMetaGenerator.CN_PK2, PrimaryKeyType.STRING)
                .addPrimaryKey(TableMetaGenerator.CN_TAMESTAMP_NAME, PrimaryKeyType.INTEGER)
                .setMaxVersion(1)
                .setTTL(-1, TimeUnit.SECONDS)
                .commitIgnore();

        wrapper.createTableAfter();

        for (long i = 0; i < 10000; i += 10) {
            wrapper.putRow()
                    .addPrimaryKey(TableMetaGenerator.CN_PK0, "md5")
                    .addPrimaryKey(TableMetaGenerator.CN_PK1, "cpu")
                    .addPrimaryKey(TableMetaGenerator.CN_PK2, "[Cluster=AY45W,Role=OTSServer#]")
                    .addPrimaryKey(TableMetaGenerator.CN_TAMESTAMP_NAME, TimeUnit.SECONDS.toMicros(i))
                    .addColumn("load1", i)
                    .addColumn("load5", i)
                    .addColumn("load15", i)
                    .commit();
        }


        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(table);
        rangeRowQueryCriteria.setMaxVersions(1);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(PrimaryKeyWrapper.instance()
                .addPrimaryKey(TableMetaGenerator.CN_PK0, "md5")
                .addPrimaryKey(TableMetaGenerator.CN_PK1, "cpu")
                .addPrimaryKey(TableMetaGenerator.CN_PK2, "[Cluster=AY45W,Role=OTSServer#]")
                .addPrimaryKey(TableMetaGenerator.CN_TAMESTAMP_NAME, 0)
                .get());
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(PrimaryKeyWrapper.instance()
                .addPrimaryKey(TableMetaGenerator.CN_PK0, "md5")
                .addPrimaryKey(TableMetaGenerator.CN_PK1, "cpu")
                .addPrimaryKey(TableMetaGenerator.CN_PK2, "[Cluster=AY45W,Role=OTSServer#]")
                .addPrimaryKey(TableMetaGenerator.CN_TAMESTAMP_NAME, TimeUnit.SECONDS.toMicros(20000))
                .get());

        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(rangeRowQueryCriteria);
        GetRangeIterator iterator = new GetRangeIterator(client, request);

        TimestreamIdentifier meta = new TimestreamIdentifier.Builder("cpu")
                .addTag("Cluster", "AY45W")
                .addTag("Role", "OTSServer#")
                .build();

        PointIterator pointIterator = new PointIterator(iterator, meta);

        List<Point> pointList = new ArrayList<Point>(1000);
        while(pointIterator.hasNext()) {
            pointList.add(pointIterator.next());
        }

        Assert.assertEquals(1000, pointList.size());

        for (int i = 0; i < 1000; i++) {
            Point p = pointList.get(i);
            Assert.assertEquals(3, pointList.get(i).getFields().size());
            Assert.assertEquals(TimeUnit.SECONDS.toMicros(i * 10), p.getTimestamp());
        }
    }
}
