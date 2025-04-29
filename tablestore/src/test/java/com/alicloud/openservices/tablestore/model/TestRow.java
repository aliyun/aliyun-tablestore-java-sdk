package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRow {

    @Test
    public void testConstructorWithInvalidArguments() {
        try {
            new Row(null, new ArrayList<Column>());
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new Row(new PrimaryKey(new PrimaryKeyColumn[0]), new ArrayList<Column>());
            fail();
        } catch (IllegalArgumentException e) {

        }

        Row row = new Row(PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(100)).build(),
                new ArrayList<Column>());
        assertTrue(row.isEmpty());
        assertTrue(row.getColumn("Column0").isEmpty());
        assertTrue(row.getLatestColumn("Column0") == null);
        assertTrue(row.getColumns().length == 0);
        assertTrue(row.getColumnsMap().isEmpty());
    }

    @Test
    public void testOperations() {
        List<Column> columns = new ArrayList<Column>();
        Random random = new Random(System.currentTimeMillis());

        columns.add(new Column("Column0", ColumnValue.fromLong(11), 1418610011));
        columns.add(new Column("Column0", ColumnValue.fromLong(10), 1418610010));
        columns.add(new Column("Column0", ColumnValue.fromLong(9), 1418610009));

        columns.add(new Column("Column1", ColumnValue.fromLong(15), 1418610015));
        columns.add(new Column("Column1", ColumnValue.fromLong(14), 1418610014));
        columns.add(new Column("Column1", ColumnValue.fromLong(13), 1418610013));

        columns.add(new Column("Column2", ColumnValue.fromLong(13), 1418610013));
        columns.add(new Column("Column2", ColumnValue.fromLong(12), 1418610012));

        columns.add(new Column("Column3", ColumnValue.fromLong(17), 1418610017));
        columns.add(new Column("Column3", ColumnValue.fromLong(16), 1418610016));
        columns.add(new Column("Column3", ColumnValue.fromLong(13), 1418610013));

        columns.add(new Column("Column4", ColumnValue.fromLong(19), 1418610019));
        columns.add(new Column("Column4", ColumnValue.fromLong(18), 1418610018));
        columns.add(new Column("Column4", ColumnValue.fromLong(17), 1418610017));
        columns.add(new Column("Column4", ColumnValue.fromLong(15), 1418610015));

        columns.add(new Column("Column5", ColumnValue.fromLong(15), 1418610015));
        columns.add(new Column("Column5", ColumnValue.fromLong(14), 1418610014));
        columns.add(new Column("Column5", ColumnValue.fromLong(13), 1418610013));
        columns.add(new Column("Column5", ColumnValue.fromLong(11), 1418610011));

        List<Column> randomColumns = new ArrayList<Column>(columns);
        Collections.shuffle(randomColumns, random);
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(10))
                .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromString("A")).build();

        Row row = new Row(primaryKey, randomColumns);
        Column[] rowColumns = row.getColumns();

        assertEquals(rowColumns.length, columns.size());
        for (int i = 0; i < rowColumns.length; i++) {
            assertEquals(columns.get(i), rowColumns[i]);
        }

        assertEquals(row.getPrimaryKey(), primaryKey);

        assertTrue(row.getColumn("Column6").isEmpty());
        assertTrue(row.getColumn("NonExistColumn").isEmpty());

        List<Column> getColumns = row.getColumn("Column0");
        assertEquals(getColumns.size(), 3);
        assertEquals(getColumns.get(0), columns.get(0));
        assertEquals(getColumns.get(1), columns.get(1));
        assertEquals(getColumns.get(2), columns.get(2));

        getColumns = row.getColumn("Column1");
        assertEquals(getColumns.size(), 3);
        assertEquals(getColumns.get(0), columns.get(3));
        assertEquals(getColumns.get(1), columns.get(4));
        assertEquals(getColumns.get(2), columns.get(5));

        getColumns = row.getColumn("Column2");
        assertEquals(getColumns.size(), 2);
        assertEquals(getColumns.get(0), columns.get(6));
        assertEquals(getColumns.get(1), columns.get(7));

        getColumns = row.getColumn("Column3");
        assertEquals(getColumns.size(), 3);
        assertEquals(getColumns.get(0), columns.get(8));
        assertEquals(getColumns.get(1), columns.get(9));
        assertEquals(getColumns.get(2), columns.get(10));

        getColumns = row.getColumn("Column4");
        assertEquals(getColumns.size(), 4);
        assertEquals(getColumns.get(0), columns.get(11));
        assertEquals(getColumns.get(1), columns.get(12));
        assertEquals(getColumns.get(2), columns.get(13));
        assertEquals(getColumns.get(3), columns.get(14));

        getColumns = row.getColumn("Column5");
        assertEquals(getColumns.size(), 4);
        assertEquals(getColumns.get(0), columns.get(15));
        assertEquals(getColumns.get(1), columns.get(16));
        assertEquals(getColumns.get(2), columns.get(17));
        assertEquals(getColumns.get(3), columns.get(18));

        assertTrue(row.getLatestColumn("Column6") == null);
        assertTrue(row.getLatestColumn("NonExistColumn") == null);

        assertEquals(row.getLatestColumn("Column0"), columns.get(0));
        assertEquals(row.getLatestColumn("Column1"), columns.get(3));
        assertEquals(row.getLatestColumn("Column2"), columns.get(6));
        assertEquals(row.getLatestColumn("Column3"), columns.get(8));
        assertEquals(row.getLatestColumn("Column4"), columns.get(11));
        assertEquals(row.getLatestColumn("Column5"), columns.get(15));

        assertTrue(row.contains("Column0"));
        assertTrue(row.contains("Column1"));
        assertTrue(row.contains("Column2"));
        assertTrue(row.contains("Column3"));
        assertTrue(row.contains("Column4"));
        assertTrue(row.contains("Column5"));

        assertTrue(!row.contains("Column6"));
        assertTrue(!row.contains("NonExistColumn"));

        assertTrue(!row.isEmpty());

        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();
        assertEquals(columnsMap.size(), 6);
        assertTrue(columnsMap.get("Column0") != null);
        assertTrue(columnsMap.get("Column1") != null);
        assertTrue(columnsMap.get("Column2") != null);
        assertTrue(columnsMap.get("Column3") != null);
        assertTrue(columnsMap.get("Column4") != null);
        assertTrue(columnsMap.get("Column5") != null);

        assertEquals(columnsMap.ceilingKey("Column"), "Column0");
        assertEquals(columnsMap.ceilingKey("Column0"), "Column0");
        assertEquals(columnsMap.ceilingKey("Column00"), "Column1");

        assertEquals(columnsMap.ceilingKey("Column1"), "Column1");
        assertEquals(columnsMap.ceilingKey("Column10"), "Column2");

        assertEquals(columnsMap.ceilingKey("Column2"), "Column2");
        assertEquals(columnsMap.ceilingKey("Column20"), "Column3");

        assertEquals(columnsMap.ceilingKey("Column3"), "Column3");
        assertEquals(columnsMap.ceilingKey("Column30"), "Column4");

        assertEquals(columnsMap.ceilingKey("Column4"), "Column4");
        assertEquals(columnsMap.ceilingKey("Column40"), "Column5");

        assertEquals(columnsMap.ceilingKey("Column5"), "Column5");
        assertTrue(columnsMap.ceilingKey("Column50") == null);

        String[] descendingKeys = columnsMap.descendingKeySet().toArray(new String[columnsMap.keySet().size()]);
        assertEquals(descendingKeys.length, 6);
        assertEquals(descendingKeys[0], "Column5");
        assertEquals(descendingKeys[1], "Column4");
        assertEquals(descendingKeys[2], "Column3");
        assertEquals(descendingKeys[3], "Column2");
        assertEquals(descendingKeys[4], "Column1");
        assertEquals(descendingKeys[5], "Column0");

        NavigableMap<Long, ColumnValue> values = columnsMap.get("Column0");
        compareTimestamps(values, values.descendingKeySet().toArray(new Long[values.keySet().size()]),
                new long[]{1418610009, 1418610010, 1418610011});

        values = columnsMap.get("Column1");
        compareTimestamps(values, values.descendingKeySet().toArray(new Long[values.keySet().size()]),
                new long[]{1418610013, 1418610014, 1418610015});

        values = columnsMap.get("Column2");
        compareTimestamps(values, values.descendingKeySet().toArray(new Long[values.keySet().size()]),
                new long[]{1418610012, 1418610013});

        values = columnsMap.get("Column3");
        compareTimestamps(values, values.descendingKeySet().toArray(new Long[values.keySet().size()]),
                new long[]{1418610013, 1418610016, 1418610017});

        values = columnsMap.get("Column4");
        compareTimestamps(values, values.descendingKeySet().toArray(new Long[values.keySet().size()]),
                new long[]{1418610015, 1418610017, 1418610018, 1418610019});

        values = columnsMap.get("Column5");
        compareTimestamps(values, values.descendingKeySet().toArray(new Long[values.keySet().size()]),
                new long[]{1418610011, 1418610013, 1418610014, 1418610015});
    }

    private void compareTimestamps(NavigableMap<Long, ColumnValue> values, Long[] l1, long[] l2) {
        assertEquals(l1.length, l2.length);
        for (int i = 0; i < l1.length; i++) {
            assertEquals(l1[i].longValue(), l2[i]);
            assertEquals(values.get(l1[i]).asLong(), l2[i] - 1418610000);
        }
    }

    @Test
    public void testRowCompareTo() {

    }
}
