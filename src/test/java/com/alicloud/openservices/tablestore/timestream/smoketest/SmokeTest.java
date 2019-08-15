package com.alicloud.openservices.tablestore.timestream.smoketest;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.timestream.*;
import com.alicloud.openservices.tablestore.timestream.model.*;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.alicloud.openservices.tablestore.timestream.bench.Conf;
import com.alicloud.openservices.tablestore.timestream.model.filter.Filter;
import com.alicloud.openservices.tablestore.timestream.model.filter.Name;

public class SmokeTest {

    public static void main(String[] args) throws FileNotFoundException,InterruptedException {
        Conf conf = Conf.newInstance("src/test/resources/test_conf.json");

        String databaseName = "smoketest";

        TimestreamDBConfiguration config = new TimestreamDBConfiguration(databaseName);
        AsyncClient asyncClient = new AsyncClient(
                conf.getEndpoint(),
                conf.getAccessId(),
                conf.getAccessKey(),
                conf.getInstance());
        TimestreamDB client = new TimestreamDBClient(
                asyncClient, config);
        client.createMetaTable();
        String tableName = "datatable_1";
        client.createDataTable(tableName);

        try {
            TimestreamMetaTable metaWriter = client.metaTable();
            TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu")
                    .addTag("machine", "123.et2")
                    .addTag("cluster", "45c")
                    .build();
            TimestreamMeta meta = new TimestreamMeta(identifier)
                    .addAttribute("TableStore.role#", "")
                    .addAttribute("OTS.role#", "");
            // ---------------------------------------------------
            // 写入时间线
            // ---------------------------------------------------
            metaWriter.put(meta);

            // sleep 1s for sync index
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));

            TimestreamMetaTable metaReader = client.metaTable();
            Filter filter = Name.equal("cpu");

            // ---------------------------------------------------
            // 获取cpu下的所有时间线Meta
            // ---------------------------------------------------
            Iterator<TimestreamMeta> iterator = metaReader.filter(filter).fetchAll();
            while (iterator.hasNext()) {
                TimestreamMeta metaOut = iterator.next();
                System.out.print(metaOut.toString());
            }

            TimestreamDataTable dataWriter = client.dataTable(tableName);
            // ---------------------------------------------------
            // 通过Writer写入100个Point
            // ---------------------------------------------------
            for (int i = 0; i < 100; i++) {
                dataWriter.write(meta.getIdentifier(), new Point.Builder(i, TimeUnit.SECONDS)
                        .addField("load1", i)
                        .addField("load5", i)
                        .addField("load15", i).build());
            }

            TimestreamDataTable dataReader = client.dataTable("datatable_1");
            // ---------------------------------------------------
            // 通过Reader读取这个时间线下的Point
            // ---------------------------------------------------
            int count = 0;
            Iterator<Point> pointIterator =  dataReader.get(meta.getIdentifier()).timeRange(TimeRange.range(0, 100, TimeUnit.SECONDS)).fetchAll();
            while (pointIterator.hasNext()) {
                Point point = pointIterator.next();
                System.out.println(point.getTimestamp() + ":" + point.toString());
                count++;
            }

            System.out.println("Total point: " + count);

        } finally {
            client.close();
        }
    }
}
