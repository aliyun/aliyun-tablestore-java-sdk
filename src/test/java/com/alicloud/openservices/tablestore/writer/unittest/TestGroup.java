package com.alicloud.openservices.tablestore.writer.unittest;


import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.writer.Group;
import com.alicloud.openservices.tablestore.writer.WriterResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestGroup {

    @Test
    public void testGroupOnComplete() {
        int groupSize = 200;
        String exceptionMessage = "mock exception";

        Group group = new Group(groupSize);
        for (int i = 0; i < groupSize; i++) {
            PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i));
            if (i % 2 == 0) {
                group.failedOneRow(new RowPutChange("tableName", builder.build()), new Exception(exceptionMessage));
            } else {
                group.succeedOneRow(new RowDeleteChange("tableName", builder.build()));
            }

        }

        try {
            WriterResult result = group.getFuture().get();

            Assert.assertTrue(result.isAllFinished());
            Assert.assertFalse(result.isAllSucceed());
            Assert.assertEquals(result.getTotalCount(), groupSize);

            Assert.assertEquals(result.getSucceedRows().size(), groupSize / 2);
            for (WriterResult.RowChangeStatus status : result.getSucceedRows()) {
                Assert.assertTrue(status.isSucceed());
                Assert.assertNull(status.getException());
                Assert.assertNotNull(status.getRowChange());
            }

            Assert.assertEquals(result.getFailedRows().size(), groupSize / 2);
            for (WriterResult.RowChangeStatus status : result.getFailedRows()) {
                Assert.assertFalse(status.isSucceed());
                Assert.assertNotNull(status.getException());
                Assert.assertEquals(status.getException().getMessage(), exceptionMessage);
            }

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGroupOnCompleteWithDirtyRow() {
        int groupSize = 200;
        int dirtyIndex = 100;
        String exceptionMessage = "DirtyRow";

        Group group = new Group(groupSize);
        for (int i = 0; i < groupSize; i++) {
            PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i));
            if (i == dirtyIndex) {
                group.failedOneRow(new RowPutChange("tableName", builder.build()), new Exception(exceptionMessage));
            } else {
                group.succeedOneRow(new RowDeleteChange("tableName", builder.build()));
            }

        }

        try {
            WriterResult result = group.getFuture().get();

            Assert.assertTrue(result.isAllFinished());
            Assert.assertFalse(result.isAllSucceed());
            Assert.assertEquals(result.getTotalCount(), groupSize);

            Assert.assertEquals(result.getFailedRows().size(), 1);
            for (WriterResult.RowChangeStatus status : result.getFailedRows()) {
                Assert.assertFalse(status.isSucceed());
                Assert.assertNotNull(status.getException());
                Assert.assertEquals(status.getException().getMessage(), exceptionMessage);
            }

            Assert.assertEquals(result.getSucceedRows().size(), groupSize - 1);
            for (WriterResult.RowChangeStatus status : result.getSucceedRows()) {
                Assert.assertTrue(status.isSucceed());
                Assert.assertNull(status.getException());
                Assert.assertNotSame(status.getRowChange().getPrimaryKey().getPrimaryKeyColumn(0).getValue().asLong(), dirtyIndex);
            }

        } catch (Exception e) {
            Assert.fail();
        }
    }


    @Test
    public void testGroupFinishMore() {
        int groupSize = 200;
        String exceptionMessage = "mock exception";

        Group group = new Group(groupSize);
        for (int i = 0; i < groupSize; i++) {
            PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i));
            if (i % 3 == 0) {
                group.failedOneRow(new RowPutChange("tableName", builder.build()), new Exception(exceptionMessage));
            } else {
                group.succeedOneRow(new RowDeleteChange("tableName", builder.build()));
            }

        }

        try {
            group.succeedOneRow(null);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertEquals(e.getMessage().split("] ")[1], "WriterResult shouldn't finish more rows than total count");
            Assert.assertEquals(e.getMessage().split("] ")[0], "[" + group.getGroupId());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGroupFinishLess() {
        int groupSize = 200;
        long timeoutValue = 901111;
        String exceptionMessage = "Mock Timeout";

        Group group = new Group(groupSize);
        for (int i = 0; i < groupSize - 1; i++) {
            PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i));

            group.succeedOneRow(new RowDeleteChange("tableName", builder.build()));
        }

        try {
            group.getFuture().get(1000, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (Exception e) {
            Assert.fail();
        }

        PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(timeoutValue));
        group.failedOneRow(new RowDeleteChange("tableName", builder.build()), new Exception(exceptionMessage));

        try {
            WriterResult result = group.getFuture().get();
            Assert.assertTrue(result.isAllFinished());
            Assert.assertFalse(result.isAllSucceed());

            Assert.assertEquals(result.getFailedRows().size(), 1);
            for (WriterResult.RowChangeStatus status : result.getFailedRows()) {
                Assert.assertFalse(status.isSucceed());
                Assert.assertEquals(status.getException().getMessage(), exceptionMessage);
                Assert.assertEquals(status.getRowChange().getPrimaryKey().getPrimaryKeyColumn(0).getValue().asLong(), timeoutValue);
            }
        } catch (Exception e) {
            Assert.fail();
        }
    }

}
