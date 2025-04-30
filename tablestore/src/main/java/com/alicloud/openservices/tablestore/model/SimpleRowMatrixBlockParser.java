package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferCrc8;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static com.alicloud.openservices.tablestore.core.Constants.UTF8_CHARSET;

/**
 * Data block based on rows for the range scan scenario
 */
public class SimpleRowMatrixBlockParser {

    private final ByteBuffer buffer;
    private final int apiVersion;
    private final int dataOffset;
    private final int optionsOffset;
    private final int pkCount;
    private final int attrCount;
    private final int fieldCount;
    private final int footerOffset;
    private final int totalBufferBytes;
    private final boolean hasEntirePrimaryKeys;
    private final int rowCount;
    private final int fieldNameArrayOffset;
    private final int[] fieldDataOffsetArray;// Includes primary key and non-primary key
    private String[] fieldNames;

    private int nextRowIndex = 0;
    private int nextRowOffset;

    public SimpleRowMatrixBlockParser(ByteBuffer buffer) {
        buffer.rewind();
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        this.buffer = buffer;
        this.totalBufferBytes = buffer.limit();

        // parse fixed fields
        apiVersion = buffer.getInt();
        ensureEqual(apiVersion, SimpleRowMatrixBlockConstants.API_VERSION, "apiVersion");

        dataOffset = buffer.getInt();
        ensureNonNegative(dataOffset, "dataOffset");

        optionsOffset = buffer.getInt();
        ensureNonNegative(optionsOffset, "optionsOffset");

        pkCount = buffer.getInt();
        ensureNonNegative(pkCount, "pkCount");
        attrCount = buffer.getInt();
        ensureNonNegative(attrCount, "attrCount");
        fieldCount = pkCount + attrCount;

        // only record, do not parse names until have to
        fieldNameArrayOffset = buffer.position();

        // parse options
        buffer.position(optionsOffset);

        final byte tagEntirePrimaryKeys = buffer.get();
        ensureEqual(tagEntirePrimaryKeys, SimpleRowMatrixBlockConstants.TAG_ENTIRE_PRIMARY_KEYS, "tagEntirePrimaryKeys");
        final byte hasEntirePrimaryKeys = buffer.get();
        if (hasEntirePrimaryKeys != 0 && hasEntirePrimaryKeys != 1) {
            throw new ClientException("Invalid hasEntirePrimaryKeys value:" + hasEntirePrimaryKeys);
        }
        this.hasEntirePrimaryKeys = (hasEntirePrimaryKeys == 1);

        final byte tagRowCount = buffer.get();
        ensureEqual(tagRowCount, SimpleRowMatrixBlockConstants.TAG_ROW_COUNT, "tagRowCount");
        rowCount = buffer.getInt();
        ensureNonNegative(rowCount, "rowCount");

        // parse footer
        buffer.position(totalBufferBytes - 2);
        final byte tagCheckSum = buffer.get();
        ensureEqual(tagCheckSum, SimpleRowMatrixBlockConstants.TAG_CHECKSUM, "tagCheckSum");
        final byte crc = buffer.get();
        final byte expectCrc = calcCheckSum();
        ensureEqual(crc, expectCrc, "checksum");

        // Initialize in advance, as each subsequent line will change.
        fieldDataOffsetArray = new int[fieldCount];

        // init footer
        footerOffset = this.totalBufferBytes - 2;

        // Read data starting point
        nextRowOffset = dataOffset;
    }

    private static void ensureEqual(byte actual, byte expect, String fieldName) {
        if (actual != expect) {
            throw new ClientException(fieldName + " mismatch. Actual:" + actual +
                    ". Expect:" + expect);
        }
    }

    private static void ensureEqual(int actual, int expect, String fieldName) {
        if (actual != expect) {
            throw new ClientException(fieldName + " mismatch. Actual:" + actual +
                    ". Expect:" + expect);
        }
    }

    private static void ensureNonNegative(int value, String fieldName) {
        if (value < 0) {
            throw new ClientException(fieldName + " can't be negative. Now:" + value);
        }
    }

    public String[] parseFieldNames() {
        if (fieldNames == null) {
            fieldNames = new String[fieldCount];
            buffer.position(fieldNameArrayOffset);
            for (int i = 0; i < fieldCount; i++) {
                short nameLength = buffer.getShort();
                ensureNonNegative(nameLength, "nameLength");
                fieldNames[i] = retrieveString(nameLength);
            }
        }
        return fieldNames;
    }

    public int getPkCount() {
        return pkCount;
    }

    public int getAttrCount() {
        return attrCount;
    }

    public int getFieldCount() {
        return fieldCount;
    }

    public boolean hasEntirePrimaryKeys() {
        return hasEntirePrimaryKeys;
    }

    public int getRowCount() {
        return rowCount;
    }

    public boolean hasNext() {
        return nextRowOffset < footerOffset;
    }

    public int next() {
        if (!hasNext()) {
            throw new ClientException("SimpleRowMatrixBlockParser has no next");
        }
        buffer.position(nextRowOffset);
        final byte tagRow = buffer.get();
        ensureEqual(tagRow, SimpleRowMatrixBlockConstants.TAG_ROW, "tagRow");

        for (int i = 0; i < fieldCount; i++) {
            fieldDataOffsetArray[i] = buffer.position();
            final byte type = buffer.get();
            skipColumn(toColumnType(type));
        }

        nextRowOffset = buffer.position();
        return nextRowIndex++;// Return first
    }

    public int getTotalBytes() {
        return totalBufferBytes;
    }

    /**
     * The total number of data bytes, not including header information, representing the pure amount of data that has already been fetched from the server.
     */
    public int getDataBytes() {
        return footerOffset - dataOffset;
    }

    public boolean isNull(int fieldIndex) {
        return getColumnType(fieldIndex) == null;
    }

    public boolean getBoolean(int fieldIndex) {
        ColumnType type = getColumnType(fieldIndex);
        if (type != ColumnType.BOOLEAN) {
            throw new ClientException("ColumnType mismatch. Actual:" + type +
                    ". Expect:" + ColumnType.BOOLEAN);
        }

        final byte value = buffer.get();
        if (value != 0 && value != 1) {
            throw new ClientException("Invalid value for boolean:" + value);
        }
        return value != 0;
    }

    public long getLong(int fieldIndex) {
        ColumnType type = getColumnType(fieldIndex);
        if (type != ColumnType.INTEGER) {
            throw new ClientException("ColumnType mismatch. Actual:" + type +
                    ". Expect:" + ColumnType.INTEGER);
        }
        return buffer.getLong();
    }

    public double getDouble(int fieldIndex) {
        ColumnType type = getColumnType(fieldIndex);
        if (type != ColumnType.DOUBLE) {
            throw new ClientException("ColumnType mismatch. Actual:" + type +
                    ". Expect:" + ColumnType.DOUBLE);
        }

        return buffer.getDouble();
    }

    public String getString(int fieldIndex) {
        ColumnType type = getColumnType(fieldIndex);
        if (type != ColumnType.STRING) {
            throw new ClientException("ColumnType mismatch. Actual:" + type +
                    ". Expect:" + ColumnType.STRING);
        }
        int length = buffer.getInt();
        return retrieveString(length);
    }

    public byte[] getBinary(int fieldIndex) {
        ColumnType type = getColumnType(fieldIndex);
        if (type != ColumnType.BINARY) {
            throw new ClientException("ColumnType mismatch. Actual:" + type +
                    ". Expect:" + ColumnType.BINARY);
        }
        int length = buffer.getInt();
        byte[] bs = new byte[length];
        buffer.get(bs);
        return bs;
    }

    public Object getObject(int fieldIndex) {
        final ColumnType type = getColumnType(fieldIndex);
        if (type == null) {
            return null;
        }
        switch (type) {
            case STRING:
                return getString(fieldIndex);
            case BINARY:
                return getBinary(fieldIndex);
            case BOOLEAN:
                return getBoolean(fieldIndex);
            case INTEGER:
                return getLong(fieldIndex);
            case DOUBLE:
                return getDouble(fieldIndex);
        }
        throw new ClientException("Unknown columnType:" + type);
    }

    public ColumnType getColumnType(int fieldIndex) {
        buffer.position(fieldDataOffsetArray[fieldIndex]);
        byte type = buffer.get();
        return toColumnType(type);
    }

    // compatiable with old interface but will generate many objects
    // get row from current cursor
    public Row getRow() {
        List<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>(pkCount);
        parseFieldNames(); // ensure fieldNames not null
        for (int j = 0; j < pkCount; j++) {
            ColumnType type = getColumnType(j);
            switch (type) {
                case INTEGER:
                    pks.add(new PrimaryKeyColumn(fieldNames[j], PrimaryKeyValue.fromLong(getLong(j))));
                    break;
                case STRING:
                    pks.add(new PrimaryKeyColumn(fieldNames[j], PrimaryKeyValue.fromString(getString(j))));
                    break;
                case BINARY:
                    pks.add(new PrimaryKeyColumn(fieldNames[j], PrimaryKeyValue.fromBinary(getBinary(j))));
                    break;
                default:
                    throw new ClientException("Invalid ColumnType for PrimaryKeyType:" + type);
            }
        }
        List<Column> cols = new ArrayList<Column>(attrCount);
        for (int j = pkCount; j < fieldCount; j++) {
            ColumnType type = getColumnType(j);
            if (type == null) {
                // ignore null columns
                continue;
            }
            switch (type) {
                case INTEGER:
                    cols.add(new Column(fieldNames[j], ColumnValue.fromLong(getLong(j))));
                    break;
                case BOOLEAN:
                    cols.add(new Column(fieldNames[j], ColumnValue.fromBoolean(getBoolean(j))));
                    break;
                case DOUBLE:
                    cols.add(new Column(fieldNames[j], ColumnValue.fromDouble(getDouble(j))));
                    break;
                case STRING:
                    cols.add(new Column(fieldNames[j], ColumnValue.fromString(getString(j))));
                    break;
                case BINARY:
                    cols.add(new Column(fieldNames[j], ColumnValue.fromBinary(getBinary(j))));
                    break;
                default:
                    throw new ClientException("Invalid ColumnType:" + type);
            }
        }
        return new Row(new PrimaryKey(pks), cols);
    }

    // compatiable with old interface but will generate many objects
    public List<Row> getRows() {
        List<Row> rows = new ArrayList<Row>(rowCount);
        nextRowOffset = dataOffset;
        for (int i = 0; i < rowCount; i++) {
            next();
            rows.add(getRow());
        }
        return rows;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SimpleRowMatrixBlock{");
        sb.append("apiVersion=").append(apiVersion)
                .append(", pkCount=").append(pkCount)
                .append(", attrCount=").append(attrCount)
                .append(", fieldCount=").append(fieldCount)
                .append(", rowCount=").append(rowCount)
                .append(", fieldNameArrayOffset=").append(fieldNameArrayOffset)
                .append(", dataOffset=").append(dataOffset)
                .append(", footerOffset=").append(footerOffset)
                .append(", totalBufferBytes=").append(totalBufferBytes)
                .append(", hasEntirePrimaryKeys=").append(hasEntirePrimaryKeys)
                .append('}');
        return sb.toString();
    }

    private byte calcCheckSum() {
        int current = buffer.position();
        buffer.position(0);
        byte crc = 0;
        for (int i = 0; i < totalBufferBytes - 1; i++) {
            crc = PlainBufferCrc8.crc8(crc, buffer.get());
        }
        buffer.position(current);
        return crc;
    }

    private ColumnType toColumnType(byte value) {
        switch (value) {
            case 0:
                return ColumnType.INTEGER;
            case 1:
                return ColumnType.DOUBLE;
            case 2:
                return ColumnType.BOOLEAN;
            case 3:
                return ColumnType.STRING;
            case 6:
                return null;
            case 7:
                return ColumnType.BINARY;
        }
        throw new ClientException("Unsupported data type:" + value);
    }

    private String retrieveString(int length) {
        if (buffer.isReadOnly()) {
            byte[] bs = new byte[length];
            buffer.get(bs);// Do not use the array() interface directly, as it is currently a ReadOnlyBuffer and will cause an error.
            return new String(bs, UTF8_CHARSET);
        } else {
            int p = buffer.position();
            String x = new String(buffer.array(), p, length);
            buffer.position(p + length);
            return x;
        }
    }

    private void skipColumn(ColumnType type) {
        int skipLen = 0;
        if (type != null) {
            switch (type) {
                case STRING:
                case BINARY:
                    skipLen = buffer.getInt();
                    break;
                case BOOLEAN:
                    skipLen = 1;
                    break;
                case INTEGER:
                case DOUBLE:
                    skipLen = 8;
                    break;
                default:
                    throw new ClientException("Unknown columnType:" + type);
            }
        }
        buffer.position(buffer.position() + skipLen);
    }
}

