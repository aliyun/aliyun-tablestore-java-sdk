package com.alicloud.openservices.tablestore.timestream.functiontest;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.timestream.model.GetRangeIterator;
import com.alicloud.openservices.tablestore.timestream.bench.Conf;
import com.alicloud.openservices.tablestore.timestream.bench.wrapper.PrimaryKeyWrapper;
import com.alicloud.openservices.tablestore.timestream.bench.wrapper.TableStoreWrapper;
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

public class GetRangeIteratorFunctiontest {
    private static Logger logger = LoggerFactory.getLogger(GetRangeIteratorFunctiontest.class);
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
        String table = "GetRangIteratorFunctiontest";

        AsyncClient client = new AsyncClient(conf.getEndpoint(), conf.getAccessId(), conf.getAccessKey(), conf.getInstance());
        TableStoreWrapper wrapper = TableStoreWrapper.instance(client, table);
        wrapper.createTable()
                .addPrimaryKey("uid", PrimaryKeyType.INTEGER)
                .setMaxVersion(1)
                .setTTL(-1, TimeUnit.SECONDS)
                .commitIgnore();

        wrapper.createTableAfter();

        for (long i = 0; i < 10000; i++) {
            wrapper.putRow().addPrimaryKey("uid", i).addColumn("attr0", i).addColumn("attr1", i).commit();
        }

        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(table);
        rangeRowQueryCriteria.setMaxVersions(1);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(PrimaryKeyWrapper.instance().addPrimaryKey("uid", 0).get());
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(PrimaryKeyWrapper.instance().addPrimaryKey("uid", 20000).get());

        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(rangeRowQueryCriteria);
        GetRangeIterator iterator = new GetRangeIterator(client, request);

        List<Row> rowList = new ArrayList<Row>(10000);
        while (iterator.hasNext()) {
            rowList.add(iterator.next());
        }

        Assert.assertEquals(10000, rowList.size());
        Assert.assertEquals(0, rowList.get(0).getPrimaryKey().getPrimaryKeyColumn("uid").getValue().asLong());
        Assert.assertEquals(5000, rowList.get(5000).getPrimaryKey().getPrimaryKeyColumn("uid").getValue().asLong());
        Assert.assertEquals(9999, rowList.get(9999).getPrimaryKey().getPrimaryKeyColumn("uid").getValue().asLong());
    }
}
