package com.alicloud.openservices.tablestore.writer.unittest;

import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.RowWriteResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;


public class TestWriterResult {
    @Test
    public void testWriterResult() {

        CapacityUnit capacityUnit = new CapacityUnit();
        capacityUnit.setReadCapacityUnit(1);
        capacityUnit.setWriteCapacityUnit(0);
        ConsumedCapacity consumedCapacity = new ConsumedCapacity(capacityUnit);


        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(100))
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("pk2"))
                .build();
        Row row = new Row(primaryKey, Arrays.asList(new Column("col1", ColumnValue.fromDouble(1L))));

        RowWriteResult result = new RowWriteResult(consumedCapacity, row);

        Assert.assertEquals(1, result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
        Assert.assertEquals(0, result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
        Assert.assertEquals(primaryKey, result.getRow().getPrimaryKey());



        CapacityUnit newCapacityUnit = new CapacityUnit();
        newCapacityUnit.setReadCapacityUnit(0);
        newCapacityUnit.setWriteCapacityUnit(1);
        ConsumedCapacity newConsumedCapacity = new ConsumedCapacity(newCapacityUnit);

        result.setConsumedCapacity(newConsumedCapacity);
        result.setRow(null);

        Assert.assertEquals(0, result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
        Assert.assertEquals(1, result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
        Assert.assertNull(result.getRow());
    }
}
