package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestNameTimestampComparator {

    @Test
    public void testComparator_CompareName() {
        NameTimestampComparator comparator = new NameTimestampComparator();
        Column column1 = new Column("T1", ColumnValue.fromLong(10), 1418380771);
        Column column2 = new Column("T1", ColumnValue.fromLong(10), 1418380771);
        Column column3 = new Column("T0", ColumnValue.fromLong(10), 1418380771);
        Column column4 = new Column("T2", ColumnValue.fromLong(10), 1418380771);
        assertTrue(comparator.compare(column1, column2) == 0);
        assertTrue(comparator.compare(column1, column3) > 0);
        assertTrue(comparator.compare(column1, column4) < 0);
    }

    @Test
    public void testComparator_CompareTimestamp() {
        NameTimestampComparator comparator = new NameTimestampComparator();
        Column column1 = new Column("T1", ColumnValue.fromLong(10), 1418380772);
        Column column2 = new Column("T1", ColumnValue.fromLong(10), 1418380772);
        Column column3 = new Column("T1", ColumnValue.fromLong(10), 1418380773);
        Column column4 = new Column("T1", ColumnValue.fromLong(10), 1418380771);
        assertTrue(comparator.compare(column1, column2) == 0);
        assertTrue(comparator.compare(column1, column3) > 0);
        assertTrue(comparator.compare(column1, column4) < 0);
    }

    @Test
    public void testComparator_CompareValue() {
        NameTimestampComparator comparator = new NameTimestampComparator();
        Column column1 = new Column("T1", ColumnValue.fromLong(10), 1418380771);
        Column column2 = new Column("T1", ColumnValue.fromLong(11), 1418380771);
        Column column3 = new Column("T1", ColumnValue.fromLong(12), 1418380771);
        Column column4 = new Column("T1", ColumnValue.fromLong(13), 1418380771);
        assertTrue(comparator.compare(column1, column2) == 0);
        assertTrue(comparator.compare(column1, column3) == 0);
        assertTrue(comparator.compare(column1, column4) == 0);
    }

    @Test
    public void testSort() {
        List<Column> columns = new ArrayList<Column>();
        columns.add(new Column("T1", ColumnValue.fromLong(10), 1418380772));
        columns.add(new Column("T1", ColumnValue.fromLong(10), 1418380771));
        columns.add(new Column("T0", ColumnValue.fromLong(10), 1418380771));
        columns.add(new Column("T1", ColumnValue.fromLong(10), 1418380773));
        columns.add(new Column("T2", ColumnValue.fromLong(10), 1418380772));
        columns.add(new Column("T0", ColumnValue.fromLong(10), 1418380771));
        columns.add(new Column("T3", ColumnValue.fromLong(10), 1418380771));
        columns.add(new Column("T2", ColumnValue.fromLong(10), 1418380771));

        Collections.sort(columns, new NameTimestampComparator());
        assertEquals(columns.get(0).getName(), "T0");
        assertEquals(columns.get(0).getTimestamp(), 1418380771);

        assertEquals(columns.get(1).getName(), "T0");
        assertEquals(columns.get(1).getTimestamp(), 1418380771);

        assertEquals(columns.get(2).getName(), "T1");
        assertEquals(columns.get(2).getTimestamp(), 1418380773);

        assertEquals(columns.get(3).getName(), "T1");
        assertEquals(columns.get(3).getTimestamp(), 1418380772);

        assertEquals(columns.get(4).getName(), "T1");
        assertEquals(columns.get(4).getTimestamp(), 1418380771);

        assertEquals(columns.get(5).getName(), "T2");
        assertEquals(columns.get(5).getTimestamp(), 1418380772);

        assertEquals(columns.get(6).getName(), "T2");
        assertEquals(columns.get(6).getTimestamp(), 1418380771);

        assertEquals(columns.get(7).getName(), "T3");
        assertEquals(columns.get(7).getTimestamp(), 1418380771);
    }
}
