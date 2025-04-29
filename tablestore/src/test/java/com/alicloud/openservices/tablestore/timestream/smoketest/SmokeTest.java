package com.alicloud.openservices.tablestore.timestream.smoketest;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.timestream.*;
import com.alicloud.openservices.tablestore.timestream.functiontest.Helper;
import com.alicloud.openservices.tablestore.timestream.model.*;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.alicloud.openservices.tablestore.timestream.bench.Conf;
import com.alicloud.openservices.tablestore.timestream.model.filter.Filter;
import com.alicloud.openservices.tablestore.timestream.model.filter.Name;
import com.alicloud.openservices.tablestore.timestream.model.query.Sorter;

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
        List<AttributeIndexSchema> attrIndexSchemas = new ArrayList<AttributeIndexSchema>();
        attrIndexSchemas.add(new AttributeIndexSchema("OTS.role#", AttributeIndexSchema.Type.KEYWORD));
        Helper.safeClearDB(asyncClient);
        client.createMetaTable(attrIndexSchemas);
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
            // Write to the timeline
            // ---------------------------------------------------
            metaWriter.put(meta);

            // sleep 1s for sync index
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));

            TimestreamMetaTable metaReader = client.metaTable();
            Filter filter = Name.equal("cpu");

            // ---------------------------------------------------
            // Get all timeline Meta under the cpu and sort them by attributes.
            // ---------------------------------------------------
            Sorter sorter = Sorter.Builder.newBuilder()
                    .sortByAttributes("OTS.role#", Sorter.SortOrder.ASC)
                    .sortByName(Sorter.SortOrder.DESC)
                    .build();
            Iterator<TimestreamMeta> iterator = metaReader
                    .filter(filter)
                    .sort(sorter)
                    .fetchAll();
            metaReader.filter(filter).fetchAll();
            while (iterator.hasNext()) {
                TimestreamMeta metaOut = iterator.next();
                System.out.print(metaOut.toString());
            }

            TimestreamDataTable dataWriter = client.dataTable(tableName);
            // ---------------------------------------------------
            // Write 100 Points through the Writer
            // ---------------------------------------------------
            for (int i = 0; i < 100; i++) {
                dataWriter.write(meta.getIdentifier(), new Point.Builder(i, TimeUnit.SECONDS)
                        .addField("load1", i)
                        .addField("load5", i)
                        .addField("load15", i).build());
            }

            TimestreamDataTable dataReader = client.dataTable("datatable_1");
            // ---------------------------------------------------
            // Read the Points under this timeline through the Reader, and set the maximum number of items returned by the query service in one request to 100.
            // ---------------------------------------------------
            int count = 0;
            SingleColumnValueFilter fieldsFilter = new SingleColumnValueFilter(
                    "load5",
                    SingleColumnValueFilter.CompareOperator.GREATER_EQUAL,
                    new ColumnValue(10L, ColumnType.INTEGER));
            Iterator<Point> pointIterator =  dataReader.get(meta.getIdentifier())
                    .timeRange(TimeRange.range(0, 100, TimeUnit.SECONDS))
                    .filter(fieldsFilter)
                    .limit(100)
                    .descTimestamp()
                    .fetchAll();
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
