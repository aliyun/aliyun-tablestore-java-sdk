package com.alicloud.openservices.tablestore.timestream.internal;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.timestream.TimestreamRestrict;
import com.alicloud.openservices.tablestore.timestream.functiontest.Helper;
import com.alicloud.openservices.tablestore.timestream.model.Point;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamMeta;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class UtilsUnittest {

    /**
     * tags序列化和反序列化，正常case
     */
    @Test
    public void testSerializeTagsNormal() {
        {
            Random random = new Random(System.currentTimeMillis());
            Map<String, String> tags = new TreeMap<String, String>();
            tags.put("tag1", String.valueOf(random.nextInt()));
            tags.put("tag2", String.valueOf(random.nextInt()));
            tags.put("tag3", String.valueOf(random.nextInt()));
            String result = Utils.serializeTags(tags);
            System.out.print(result + "\n");

            Map<String, String> newTags = Utils.deserializeTags(result);
            Assert.assertTrue(newTags.equals(tags));
        }
        {
            // 包含字符："，=
            Map<String, String> tags = new TreeMap<String, String>();
            tags.put("tag1", String.valueOf(""));
            tags.put("tag2", String.valueOf("=1"));
            tags.put("tag3", String.valueOf("1="));
            tags.put("tag4", String.valueOf("\""));
            String result = Utils.serializeTags(tags);
            System.out.print(result + "\n");

            Map<String, String> newTags = Utils.deserializeTags(result);
            Assert.assertTrue(newTags.equals(tags));
        }
    }


    /**
     * tags序列化和反序列化，异常case
     */
    @Test
    public void testSerializeTagsAbnormal() {
        Random random = new Random(System.currentTimeMillis());
        {
            // multi tags , with one empty value
            Map<String, String> tags = new TreeMap<String, String>();
            tags.put("tag1", String.valueOf(random.nextInt()));
            tags.put("tag2", "");
            String result = Utils.serializeTags(tags);
            System.out.print(result + "\n");

            Map<String, String> newTags = Utils.deserializeTags(result);
            Assert.assertTrue(newTags.equals(tags));
        }
    }

    @Test
    public void testInvalidTagName() {
        Map<String, String> tags = new TreeMap<String, String>();
        tags.put("tag1=", "1");
        try {
            Utils.serializeTags(tags);
        } catch (ClientException e) {
            Assert.assertEquals("Illegal character(=) exist in 'tag1='.", e.getMessage());
        }
    }


    /**
     * 反序列化，异常case
     */
    @Test
    public void testDeserializeTagsAbnormal() {
        // illegal json string
        {
            String data = "[test]";
            try {
                Utils.deserializeTags(data);
                Assert.assertFalse(true);
            } catch (ClientException ex) {
                // pass
            }
        }
        // illegal item with none "="
        {
            String data = "[\"tag1\"]";
            try {
                Utils.deserializeTags(data);
                Assert.assertFalse(true);
            } catch (ClientException ex) {
                // pass
            }
        }
        // item with multi "="
        {
            String data = "[\"tag1=1=1\"]";
            try {
                Utils.deserializeTags(data);
            } catch (ClientException ex) {
                Assert.assertFalse(true);
            }
        }
    }


    @Test
    public void testTagCount() {
        TimestreamIdentifier.Builder builder = new TimestreamIdentifier.Builder("");
        for (int i = 0; i < TimestreamRestrict.TAG_COUNT; ++i) {
            builder.addTag("k" + i, "v");
            Utils.convertIdentifierToPK(builder.build());
        }
        try {
            builder.addTag("k", "v");
            Utils.convertIdentifierToPK(builder.build());
            Assert.fail();
        } catch (ClientException e) {
            // pass
        }
    }

    @Test
    public void testTagLen() {
        String key1 = "a";
        String key2 = "b";
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < (TimestreamRestrict.TAG_LEN_BYTE - 2) / 2; ++i) {
            sb.append("a");
        }
        String value = sb.toString();
        {
            TimestreamIdentifier.Builder builder = new TimestreamIdentifier.Builder("");
            builder.addTag(key1, value);
            builder.addTag(key2, value);
            Utils.convertIdentifierToPK(builder.build());
            try {
                builder.addTag("c", "");
                Utils.convertIdentifierToPK(builder.build());
                Assert.fail();
            } catch (ClientException e) {
                // pass
            }
        }
        {
            TreeMap<String, String> tags = new TreeMap<String, String>();
            tags.put(key1, value);
            tags.put(key2, value);
            {
                TimestreamIdentifier.Builder builder = new TimestreamIdentifier.Builder("");
                builder.setTags(tags);
                Utils.convertIdentifierToPK(builder.build());
            }
            tags.put("c", "");
            {
                TimestreamIdentifier.Builder builder = new TimestreamIdentifier.Builder("");
                try {
                    builder.setTags(tags);
                    Utils.convertIdentifierToPK(builder.build());
                    Assert.fail();
                } catch (ClientException e) {
                    // pass
                }
            }
        }
    }

    /**
     * ConvertMetaToPK case
     */
    @Test
    public void testConvertIdentifierToPK() {
        Random random = new Random(System.currentTimeMillis());
        TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu")
                .addTag("tag1", String.valueOf(random.nextInt()))
                .addTag("tag2", String.valueOf(random.nextInt())).build();
        String tags = Utils.serializeTags(identifier.getTags());
        PrimaryKeyBuilder pkBuilder = Utils.convertIdentifierToPK(identifier);
        PrimaryKey pks = pkBuilder.build();
        Assert.assertEquals(pks.getPrimaryKeyColumn(0).getValue().asString(), Utils.getHashCode(identifier.getName(), tags));
        Assert.assertEquals(pks.getPrimaryKeyColumn(1).getValue().asString(), identifier.getName());
        Assert.assertEquals(pks.getPrimaryKeyColumn(2).getValue().asString(), tags);
    }

    /**
     * serializeTimeSeiesMeta case
     */
    @Test
    public void testSerializeTimestreamMetaToPut() {
        Random random = new Random(System.currentTimeMillis());
        // 没有attributes
        {
            TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu")
                    .addTag("tag1", String.valueOf(random.nextInt()))
                    .addTag("tag2", String.valueOf(random.nextInt()))
                    .build();
            TimestreamMeta meta = new TimestreamMeta(identifier);
            String tags = Utils.serializeTags(identifier.getTags());
            String tableName = "test_table";
            RowChange rowChange = Utils.serializeTimestreamMetaToPut(tableName, meta);
            Assert.assertTrue(rowChange instanceof RowPutChange);
            RowPutChange rowPutChange = (RowPutChange)rowChange;
            PrimaryKey pks = rowPutChange.getPrimaryKey();
            Assert.assertEquals(pks.getPrimaryKeyColumns().length, 3);
            Assert.assertTrue(pks.getPrimaryKeyColumn(0).getName().equals(TableMetaGenerator.CN_PK0));
            Assert.assertTrue(pks.getPrimaryKeyColumn(0).getValue().asString().equals(Utils.getHashCode("cpu", tags)));
            Assert.assertTrue(pks.getPrimaryKeyColumn(1).getName().equals(TableMetaGenerator.CN_PK1));
            Assert.assertTrue(pks.getPrimaryKeyColumn(1).getValue().asString().equals("cpu"));
            Assert.assertTrue(pks.getPrimaryKeyColumn(2).getName().equals(TableMetaGenerator.CN_PK2));
            Assert.assertTrue(pks.getPrimaryKeyColumn(2).getValue().asString().equals(tags));
            Assert.assertEquals(rowPutChange.getColumnsToPut(TableMetaGenerator.CN_TAMESTAMP_NAME).get(0).getValue().asLong(), meta.getUpdateTimeInUsec());
        }
        // 有attributes
        {
            TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu")
                    .addTag("tag1", String.valueOf(random.nextInt()))
                    .addTag("tag2", String.valueOf(random.nextInt()))
                    .build();
            TimestreamMeta meta = new TimestreamMeta(identifier)
                    .addAttribute("loc", "123,456");
            String tags = Utils.serializeTags(identifier.getTags());
            String tableName = "test_table";
            RowChange rowChange = Utils.serializeTimestreamMetaToPut(tableName, meta);
            Assert.assertTrue(rowChange instanceof RowPutChange);
            RowPutChange rowPutChange = (RowPutChange)rowChange;
            PrimaryKey pks = rowPutChange.getPrimaryKey();
            Assert.assertEquals(pks.getPrimaryKeyColumns().length, 3);
            Assert.assertTrue(pks.getPrimaryKeyColumn(0).getName().equals(TableMetaGenerator.CN_PK0));
            Assert.assertTrue(pks.getPrimaryKeyColumn(0).getValue().asString().equals(Utils.getHashCode("cpu", tags)));
            Assert.assertTrue(pks.getPrimaryKeyColumn(1).getName().equals(TableMetaGenerator.CN_PK1));
            Assert.assertTrue(pks.getPrimaryKeyColumn(1).getValue().asString().equals("cpu"));
            Assert.assertTrue(pks.getPrimaryKeyColumn(2).getName().equals(TableMetaGenerator.CN_PK2));
            Assert.assertTrue(pks.getPrimaryKeyColumn(2).getValue().asString().equals(tags));
            Assert.assertEquals(rowPutChange.getColumnsToPut(TableMetaGenerator.CN_TAMESTAMP_NAME).get(0).getValue().asLong(), meta.getUpdateTimeInUsec());
            Assert.assertTrue(rowPutChange.getColumnsToPut("loc").get(0).getValue().asString().equals(meta.getAttributeAsString("loc")));
        }
    }

    /**
     * deserializeTimeSeiesMeta case
     */
    @Test
    public void testDeserializeTimestreamMeta() {
        Random random = new Random(System.currentTimeMillis());
        // row with tags, attrs
        {
            TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu")
                    .addTag("tag1", String.valueOf(random.nextInt()))
                    .addTag("tag2", String.valueOf(random.nextInt()))
                    .build();
            TimestreamMeta meta = new TimestreamMeta(identifier)
                    .addAttribute("attr1", String.valueOf(random.nextInt()))
                    .addAttribute("loc", "123,456");
            PrimaryKeyBuilder builder = Utils.convertIdentifierToPK(identifier);
            PrimaryKey pks = builder.build();
            List<Column> cols = new ArrayList<Column>();
            cols.add(new Column(TableMetaGenerator.CN_TAMESTAMP_NAME, ColumnValue.fromLong(meta.getUpdateTimeInUsec())));
            cols.add(new Column("loc", ColumnValue.fromString(meta.getAttributeAsString("loc"))));
            cols.add(new Column("attr1", ColumnValue.fromString(meta.getAttributeAsString("attr1"))));
            Row row = new Row(pks, cols);
            TimestreamMeta newMeta = Utils.deserializeTimestreamMeta(row);
            Assert.assertTrue(Helper.compareMeta(meta, newMeta));
        }
        // row with empty tags
        {
            TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu")
                    .build();
            TimestreamMeta meta = new TimestreamMeta(identifier)
                    .addAttribute("loc", "123,456");
            PrimaryKeyBuilder builder = Utils.convertIdentifierToPK(identifier);
            PrimaryKey pks = builder.build();
            List<Column> cols = new ArrayList<Column>();
            cols.add(new Column(TableMetaGenerator.CN_TAMESTAMP_NAME, ColumnValue.fromLong(meta.getUpdateTimeInUsec())));
            cols.add(new Column("loc", ColumnValue.fromString(meta.getAttributeAsString("loc"))));
            Row row = new Row(pks, cols);
            TimestreamMeta newMeta = Utils.deserializeTimestreamMeta(row);
            Assert.assertTrue(Helper.compareMeta(meta, newMeta));
        }
        // row with none location
        {
            TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu")
                    .addTag("tag1", String.valueOf(random.nextInt()))
                    .addTag("tag2", String.valueOf(random.nextInt()))
                    .build();
            TimestreamMeta meta = new TimestreamMeta(identifier);
            PrimaryKeyBuilder builder = Utils.convertIdentifierToPK(identifier);
            PrimaryKey pks = builder.build();
            List<Column> cols = new ArrayList<Column>();
            cols.add(new Column(TableMetaGenerator.CN_TAMESTAMP_NAME, ColumnValue.fromLong(meta.getUpdateTimeInUsec())));
            Row row = new Row(pks, cols);
            TimestreamMeta newMeta = Utils.deserializeTimestreamMeta(row);
            Assert.assertTrue(Helper.compareMeta(meta, newMeta));
        }
    }


    @Test
    public void testSerializeTimestreamMetaToUpdate() {
        Random random = new Random(System.currentTimeMillis());
        // 没有attributes
        {
            TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu")
                    .addTag("tag1", String.valueOf(random.nextInt()))
                    .addTag("tag2", String.valueOf(random.nextInt()))
                    .build();
            TimestreamMeta meta = new TimestreamMeta(identifier);
            String tags = Utils.serializeTags(identifier.getTags());
            String tableName = "test_table";
            RowChange rowChange = Utils.serializeTimestreamMetaToUpdate(tableName, meta);
            Assert.assertTrue(rowChange instanceof RowUpdateChange);
            RowUpdateChange rowPutChange = (RowUpdateChange)rowChange;
            PrimaryKey pks = rowPutChange.getPrimaryKey();
            Assert.assertEquals(pks.getPrimaryKeyColumns().length, 3);
            Assert.assertTrue(pks.getPrimaryKeyColumn(0).getName().equals(TableMetaGenerator.CN_PK0));
            Assert.assertTrue(pks.getPrimaryKeyColumn(0).getValue().asString().equals(Utils.getHashCode("cpu", tags)));
            Assert.assertTrue(pks.getPrimaryKeyColumn(1).getName().equals(TableMetaGenerator.CN_PK1));
            Assert.assertTrue(pks.getPrimaryKeyColumn(1).getValue().asString().equals("cpu"));
            Assert.assertTrue(pks.getPrimaryKeyColumn(2).getName().equals(TableMetaGenerator.CN_PK2));
            Assert.assertTrue(pks.getPrimaryKeyColumn(2).getValue().asString().equals(tags));
            Assert.assertEquals(rowPutChange.getColumnsToUpdate().get(0).getFirst().getValue().asLong(), meta.getUpdateTimeInUsec());
        }
        // 有attributes
        {
            TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu")
                    .addTag("tag1", String.valueOf(random.nextInt()))
                    .addTag("tag2", String.valueOf(random.nextInt()))
                    .build();
            TimestreamMeta meta = new TimestreamMeta(identifier)
                    .addAttribute("loc", "123,456");
            String tags = Utils.serializeTags(identifier.getTags());
            String tableName = "test_table";
            RowChange rowChange = Utils.serializeTimestreamMetaToUpdate(tableName, meta);
            Assert.assertTrue(rowChange instanceof RowUpdateChange);
            RowUpdateChange rowPutChange = (RowUpdateChange)rowChange;
            PrimaryKey pks = rowPutChange.getPrimaryKey();
            Assert.assertEquals(pks.getPrimaryKeyColumns().length, 3);
            Assert.assertTrue(pks.getPrimaryKeyColumn(0).getName().equals(TableMetaGenerator.CN_PK0));
            Assert.assertTrue(pks.getPrimaryKeyColumn(0).getValue().asString().equals(Utils.getHashCode("cpu", tags)));
            Assert.assertTrue(pks.getPrimaryKeyColumn(1).getName().equals(TableMetaGenerator.CN_PK1));
            Assert.assertTrue(pks.getPrimaryKeyColumn(1).getValue().asString().equals("cpu"));
            Assert.assertTrue(pks.getPrimaryKeyColumn(2).getName().equals(TableMetaGenerator.CN_PK2));
            Assert.assertTrue(pks.getPrimaryKeyColumn(2).getValue().asString().equals(tags));
            Assert.assertEquals(rowPutChange.getColumnsToUpdate().get(0).getFirst().getValue().asLong(), meta.getUpdateTimeInUsec());
            Assert.assertTrue(rowPutChange.getColumnsToUpdate().get(1).getFirst().getValue().asString().equals(meta.getAttributeAsString("loc")));
        }
    }

    @Test
    public void testSerializeTimestream() {
        Random random = new Random(System.currentTimeMillis());
        TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("cpu")
                .addTag("tag1", String.valueOf(random.nextInt()))
                .addTag("tag2", String.valueOf(random.nextInt()))
                .build();
        String tags = Utils.serializeTags(identifier.getTags());
        long now = System.currentTimeMillis();
        String tableName = "test_table";
        Point point = new Point.Builder(1000, TimeUnit.SECONDS).addField("item1", 1).addField("item2", 2).build();
        RowChange rowChange = Utils.serializeTimestream(tableName, identifier, point);
        Assert.assertTrue(rowChange instanceof RowPutChange);
        RowPutChange rowPutChange = (RowPutChange) rowChange;
        PrimaryKey pks = rowPutChange.getPrimaryKey();
        Assert.assertEquals(pks.getPrimaryKeyColumns().length, 4);
        Assert.assertTrue(pks.getPrimaryKeyColumn(0).getName().equals(TableMetaGenerator.CN_PK0));
        Assert.assertTrue(pks.getPrimaryKeyColumn(0).getValue().asString().equals(Utils.getHashCode("cpu", tags)));
        Assert.assertTrue(pks.getPrimaryKeyColumn(1).getName().equals(TableMetaGenerator.CN_PK1));
        Assert.assertTrue(pks.getPrimaryKeyColumn(1).getValue().asString().equals("cpu"));
        Assert.assertTrue(pks.getPrimaryKeyColumn(2).getName().equals(TableMetaGenerator.CN_PK2));
        Assert.assertTrue(pks.getPrimaryKeyColumn(2).getValue().asString().equals(tags));
        Assert.assertTrue(pks.getPrimaryKeyColumn(3).getName().equals(TableMetaGenerator.CN_TAMESTAMP_NAME));
        Assert.assertEquals(pks.getPrimaryKeyColumn(3).getValue().asLong(), point.getTimestamp());
        List<Column> cols = rowPutChange.getColumnsToPut();
        Assert.assertEquals(cols.size(), 2);
        Assert.assertTrue(cols.get(0).getName().equals("item1"));
        Assert.assertEquals(cols.get(0).getValue().asLong(), 1);
        Assert.assertTrue(cols.get(1).getName().equals("item2"));
        Assert.assertEquals(cols.get(1).getValue().asLong(), 2);
    }
}
