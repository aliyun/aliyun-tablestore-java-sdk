package com.alicloud.openservices.tablestore.timestream.internal;

import com.alicloud.openservices.tablestore.timestream.TimestreamRestrict;
import com.alicloud.openservices.tablestore.timestream.model.Point;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamMeta;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.ClientException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Utils {
    private static String DECOLLATOR = "=";

    public static String getHashCode(String name, String tags) {
        StringBuffer md5StrBuff = new StringBuffer();
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(name.getBytes());
            md.update(tags.getBytes());
            byte[] byteArray = md.digest();
            for (int i = 0; i < byteArray.length; i++)
            {
                if (Integer.toHexString(0xFF & byteArray[i]).length() == 1) {
                    md5StrBuff.append("0").append(Integer.toHexString(0xFF & byteArray[i]));
                } else {
                    md5StrBuff.append(Integer.toHexString(0xFF & byteArray[i]));
                }
            }
        } catch (Exception e) {
            throw new ClientException("Failed to get hash code: " + e.toString());
        }
        return md5StrBuff.toString().substring(0, 4);
    }

    private static String[] deserializeItem(String item) {
        int offset = item.indexOf(DECOLLATOR);
        String[] kv = new String[2];
        if (offset == -1) {
            throw new ClientException("Illegal attributes: " + item);
        }
        kv[0] = item.substring(0, offset);
        kv[1] = item.substring(offset+1, item.length());
        return kv;
    }

    public static String buildTagValue(String tag, String value) {
        if (tag.indexOf("=") >= 0) {
            throw new ClientException("Illegal character(=) exist in '" + tag + "'.");
        }
        return tag + DECOLLATOR + value;
    }

    public static String serializeTags(Map<String, String> tags) {
        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.disableHtmlEscaping().create();
        int tagSize = 0;
        if (tags.size() > TimestreamRestrict.TAG_COUNT) {
            throw new ClientException(String.format("The input tag count(%d) more than %d .", tags.size(), TimestreamRestrict.TAG_COUNT));
        }
        List<String> res = new ArrayList<String>();
        for (String key : tags.keySet()) {
            String tv = buildTagValue(key, tags.get(key));
            res.add(tv);
            tagSize += tv.length() - 1;
        }
        if (tagSize > TimestreamRestrict.TAG_LEN_BYTE) {
            throw new ClientException(String.format("The length(%d) of tag's name and value larger than %d.", tagSize, TimestreamRestrict.TAG_LEN_BYTE));
        }
        return gson.toJson(res);
    }

    protected static TreeMap<String, String> deserializeTags(String value) {
        TreeMap<String, String> tags = new TreeMap<String, String>();
        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.disableHtmlEscaping().create();
        List<String> res = gson.fromJson(value, new TypeToken<List<String>>(){}.getType());
        for (int i = 0; i < res.size(); ++i) {
            String[] kv = deserializeItem(res.get(i));
            tags.put(kv[0], kv[1]);
        }
        return tags;
    }

    public static PrimaryKeyBuilder convertIdentifierToPK(TimestreamIdentifier meta) {
        PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        String tags = serializeTags(meta.getTags());
        String hashcode = getHashCode(meta.getName(), tags);
        pkBuilder.addPrimaryKeyColumn(TableMetaGenerator.CN_PK0, PrimaryKeyValue.fromString(hashcode));
        pkBuilder.addPrimaryKeyColumn(TableMetaGenerator.CN_PK1, PrimaryKeyValue.fromString(meta.getName()));
        pkBuilder.addPrimaryKeyColumn(TableMetaGenerator.CN_PK2, PrimaryKeyValue.fromString(tags));
        return pkBuilder;
    }

    public static RowDeleteChange serializeTimestreamMeta(String tableName, TimestreamIdentifier meta) {
        PrimaryKeyBuilder pkBuilder = convertIdentifierToPK(meta);
        RowDeleteChange rowChange = new RowDeleteChange(tableName, pkBuilder.build());
        return rowChange;
    }

    public static RowPutChange serializeTimestreamMeta(String tableName, TimestreamMeta meta) {
        TimestreamIdentifier identifier = meta.getIdentifier();
        PrimaryKeyBuilder pkBuilder = convertIdentifierToPK(meta.getIdentifier());
        RowPutChange rowChange = new RowPutChange(tableName, pkBuilder.build());
        rowChange.addColumn(
                new Column(
                        TableMetaGenerator.CN_TAMESTAMP_NAME,
                        ColumnValue.fromLong(meta.getUpdateTimeInUsec())));
        Map<String, ColumnValue> attributes = meta.getAttributes();
        for (String key : meta.getAttributes().keySet()) {
            if (key.equals(TableMetaGenerator.CN_TAMESTAMP_NAME)) {
                throw new ClientException("Key of attribute cannot be " + TableMetaGenerator.CN_TAMESTAMP_NAME + " .");
            }
            rowChange.addColumn(key, attributes.get(key));
        }
        return rowChange;
    }

    public static TimestreamMeta deserializeTimestreamMeta(Row row) {
        PrimaryKey pk = row.getPrimaryKey();
        TimestreamIdentifier identifier = deserializeTimestreamIdentifier(row);
        TimestreamMeta meta = new TimestreamMeta(identifier);
        for (Column col : row.getColumns()) {
            String colName = col.getName();
            if (row.getColumn(colName).size() > 1) {
                throw new ClientException("Multi version exist for column: " + colName + ".");
            }
            if (colName.equals(TableMetaGenerator.CN_TAMESTAMP_NAME)) {
                long ts = col.getValue().asLong();
                meta.setUpdateTime(ts, TimeUnit.MICROSECONDS);
            } else {
                meta.addAttribute(colName, col.getValue());
            }
        }
        return meta;
    }

    public static RowUpdateChange serializeTimestreamIdentifier(String tableName, TimestreamIdentifier identifier, long timestamp) {
        PrimaryKeyBuilder pkBuilder = convertIdentifierToPK(identifier);
        RowUpdateChange rowChange = new RowUpdateChange(tableName, pkBuilder.build());
        rowChange.put(
                new Column(
                        TableMetaGenerator.CN_TAMESTAMP_NAME,
                        ColumnValue.fromLong(timestamp)));

        return rowChange;
    }

    private static TimestreamIdentifier deserializeTimestreamIdentifier(Row row) {
        PrimaryKey pk = row.getPrimaryKey();
        TimestreamIdentifier.Builder builder = new TimestreamIdentifier.Builder(
                pk.getPrimaryKeyColumn(TableMetaGenerator.CN_PK1).getValue().asString());
        String tagStr = pk.getPrimaryKeyColumn(TableMetaGenerator.CN_PK2).getValue().asString();
        builder.setTags(deserializeTags(tagStr));
        return builder.build();
    }

    public static RowPutChange serializeTimestream(String tableName, TimestreamIdentifier meta, Point point) {
        PrimaryKeyBuilder pkBuilder = convertIdentifierToPK(meta);
        pkBuilder.addPrimaryKeyColumn(
                TableMetaGenerator.CN_TAMESTAMP_NAME,
                PrimaryKeyValue.fromLong(point.getTimestamp()));
        RowPutChange rowChange = new RowPutChange(tableName, pkBuilder.build());
        for (Column col : point.getFields()) {
            rowChange.addColumn(col);
        }
        return rowChange;
    }

    public static <Res> Res waitForFuture(Future<Res> f) {
        try {
            return f.get();
        } catch(InterruptedException e) {
            throw new ClientException(
                    String.format("The thread was interrupted: %s", e.getMessage()));
        } catch(ExecutionException e) {
            throw new ClientException("The thread was aborted");
        }
    }
}
