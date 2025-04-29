package com.alicloud.openservices.tablestore.timestream.model;


import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.timestream.bench.wrapper.RowWrapper;
import com.alicloud.openservices.tablestore.timestream.model.annotation.Field;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class PointUnitTest {

    @Test
    public void testBuildByRow() {
        Row row = RowWrapper.instance()
                .addPrimaryKey("pk0", "pk0-v")
                .addPrimaryKey("pk1", 1)
                .addPrimaryKey("pk2", "hello")
                .addColumn("load1", 1)
                .addColumn("load5", 5)
                .addColumn("load15", 15)
                .get();

        Point point = new Point(100, TimeUnit.MICROSECONDS, row);
        Assert.assertEquals(point.getTimestamp(), 100);
        Assert.assertEquals(3, point.getFields().size());
        Assert.assertEquals(1, point.getField("load1").asLong());
        Assert.assertEquals(5, point.getField("load5").asLong());
        Assert.assertEquals(15, point.getField("load15").asLong());
    }

    @Test
    public void testTimestamp() {
        long timestamp = (long)(Math.random() * 10000);
        Point point = new Point.Builder(timestamp, TimeUnit.SECONDS).build();

        Assert.assertEquals(point.getTimestamp(), timestamp * 1000L* 1000L);
    }

    @Test
    public void testAddField() {
        long timestamp = (long)(Math.random() * 10000);
        Point point = new Point.Builder(timestamp, TimeUnit.SECONDS).addField("a", 123).addField("b", true).addField("c", 1.1).addField("d", "test")
                .build();
        point.addField(new Column("e", new ColumnValue("test1", ColumnType.STRING)));

        Assert.assertEquals(123, point.getField("a").asLong());
        Assert.assertEquals(true, point.getField("b").asBoolean());
        Assert.assertTrue(1.1 == point.getField("c").asDouble());
        Assert.assertEquals("test", point.getField("d").asString());
        Assert.assertEquals("test1", point.getField("e").asString());
        Assert.assertEquals(point.getFields().size(), 5);
    }

    public class SampleData {
        @Field(name="col1")
        public String col1;

        @Field(name="col2")
        public Long col2;

        @Field(name="col3")
        public Double col3;

        @Field(name="col4")
        public Boolean col4;

        @Field(name="col5")
        private String col5; // private field cannot be serilized

        public SampleData() {}

        public SampleData setCol5(String value) {
            col5 = value;
            return this;
        }

        public String getCol5() {
            return this.col5;
        }
    }

    @Test
    public void testSerilizeObject() {
        SampleData data = new SampleData();
        data.col1 = "test";
        data.col2 = (long)123;
        data.col3 = 12.3;
        data.col4 = true;

        long timestamp = (long)(Math.random() * 10000);
        Point point = new Point.Builder(timestamp, TimeUnit.SECONDS).from(data).build();
        Assert.assertEquals(point.getFields().size(), 4);
        Assert.assertEquals(point.getField("col1").asString(), data.col1);
        Assert.assertEquals(point.getField("col2").asLong(), data.col2.longValue());
        Assert.assertTrue(point.getField("col3").asDouble() == data.col3.doubleValue());
        Assert.assertEquals(point.getField("col4").asBoolean(), data.col4);
    }
}