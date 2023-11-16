package com.alicloud.openservices.tablestore.core.protocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.core.utils.BinaryUtil;
import com.alicloud.openservices.tablestore.core.utils.Pair;

import static com.alicloud.openservices.tablestore.core.protocol.PlainBufferConsts.VT_DATETIME;

public class PlainBufferBuilder {

    public static final int computePrimaryKeyValue(PrimaryKeyValue value) throws IOException {
        int size = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE;
        size += computePrimaryKeyValueWithoutLengthPrefix(value);
        return size;
    }

    public static final int computeColumnValue(ColumnValue value) throws IOException {
        int size = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE;
        size += computeColumnValueWithoutLengthPrefix(value);
        return size;
    }

    // Bytes Data: value_type + type
    public static final int computePrimaryKeyValueWithoutLengthPrefix(PrimaryKeyValue value) throws IOException {
        int size = 1; // length + type + value
        if (value.isInfMin() || value.isInfMax() || value.isPlaceHolderForAutoIncr()) {
            return size; // inf value and AutoIncr only has a type.
        }

        switch (value.getType()) {
            case STRING: {
                size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE;
                size += value.asStringInBytes().length;
                break;
            }
            case DATETIME:
            case INTEGER: {
                size += PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE;
                break;
            }
            case BINARY: {
                size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE;
                size += value.asBinary().length;
                break;
            }
            default:
                throw new IOException("Bug: unsupported primary key type: " + value.getType());

        }
        return size;
    }

    // Bytes Data: value_type + type
    public static final int computeColumnValueWithoutLengthPrefix(ColumnValue value) throws IOException {
        int size = 1; // length + type + value
        switch (value.getType()) {
            case STRING: {
                size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE;
                size += value.asStringInBytes().length;
                break;
            }
            case DATETIME:
            case INTEGER: {
                size += PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE;
                break;
            }
            case BINARY: {
                size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE;
                size += value.asBinary().length;
                break;
            }
            case DOUBLE: {
                size += PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE;
                break;
            }
            case BOOLEAN: {
                size += 1;
                break;
            }
            default:
                throw new IOException("Bug: unsupported column type: " + value.getType());

        }
        return size;
    }

    public static final int computePlainBufferExtension(PlainBufferExtension extension) {
        int size = 1; //TAG_EXTENSION
        size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // Length
        if (extension.hasSeq()) {
            size += computePlainBufferSequenceInfo();
        }
        return size;
    }

    public static final int computePlainBufferSequenceInfo() {
        int size = 1;//TAG_SEQ_INFO
        size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // Length
        size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // TAG_SEQ_INFO_EPOCH + epoch
        size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE; // TAG_SEQ_INFO_TS + timestamp
        size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // TAG_SEQ_INFO_ROW_INDEX + rowIndex
        return size;
    }

    public static final int computePlainBufferCell(PlainBufferCell cell) throws IOException {
        int size = 1; // TAG_CELL
        if (cell.hasCellName()) {
            size += 1; // TAG_CELL_NAME
            size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // length
            size += cell.getNameRawData().length;
        }
        if (cell.hasCellValue()) {
            size += 1; // TAG_CELL_VALUE
            if (cell.isPk()) {
                size += computePrimaryKeyValue(cell.getPkCellValue());
            } else {
                size += computeColumnValue(cell.getCellValue());
            }
        }
        if (cell.hasCellType()) {
            size += 2; // TAG_CELL_OP_TYPE + type
        }
        if (cell.hasCellTimestamp()) {
            size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE; // TAG_CELL_TIMESTAMP + timestamp
        }
        size += 2; // TAG_CELL_CHECKSUM + checksum
        return size;
    }

    public static final int computePlainBufferRow(PlainBufferRow row) throws IOException {
        int size = 0;
        size += 1; // TAG_ROW_PK
        for (PlainBufferCell cell : row.getPrimaryKey()) {
            size += computePlainBufferCell(cell);
        }
        if (!row.getCells().isEmpty()) {
            size += 1; // TAG_ROW_DATA
            for (PlainBufferCell cell : row.getCells()) {
                size += computePlainBufferCell(cell);
            }
        }
        if (row.hasDeleteMarker()) {
            size += 1; // TAG_DELETE_MARKER
        }
        if (row.hasExtension()) {
            size += computePlainBufferExtension(row.getExtension());
        }
        size += 2; // TAG_ROW_CHECKSUM + checksum
        return size;
    }

    public static final int computePlainBufferRowWithHeader(PlainBufferRow row) throws IOException {
        int size = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // header
        size += computePlainBufferRow(row);
        return size;
    }


    public static final int computeSkipLengthForExtensionTag(PlainBufferExtension extension) {
        int size = 0;
        if (extension.hasSeq()) {
            size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; //TAG_SEQ_ING + length
            size += computeSkipLengthForSequenceInfo();
        }
        return size;
    }

    public static int computeSkipLengthForSequenceInfo() {
        int size = 0;
        size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // TAG_SEQ_INFO_EPOCH + epoch
        size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE; // TAG_SEQ_INFO_TS + timestamp
        size += 1 + PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // TAG_SEQ_INFO_ROW_INDEX + rowIndex
        return size;
    }

    public static byte[] buildPrimaryKeyValueWithoutLengthPrefix(PrimaryKeyValue value) throws IOException {
        int size = computePrimaryKeyValueWithoutLengthPrefix(value);
        PlainBufferOutputStream output = new PlainBufferOutputStream(size);
        PlainBufferCodedOutputStream codedOutput = new PlainBufferCodedOutputStream(output);
        codedOutput.writePrimaryKeyValueWithoutLengthPrefix(value);

        if (!output.isFull()) {
            throw new IOException("Bug: serialize primary key value failed.");
        }
        return output.getBuffer();
    }

    public static byte[] buildColumnValueWithoutLengthPrefix(ColumnValue value) throws IOException {
        int size = computeColumnValueWithoutLengthPrefix(value);
        PlainBufferOutputStream output = new PlainBufferOutputStream(size);
        PlainBufferCodedOutputStream codedOutput = new PlainBufferCodedOutputStream(output);
        codedOutput.writeColumnValueWithoutLengthPrefix(value);

        if (!output.isFull()) {
            throw new IOException("Bug: serialize column value failed.");
        }
        return output.getBuffer();
    }

    public static final int computePrimaryKeyColumn(PrimaryKeyColumn column) throws IOException {
        int size = 2; // TAG_CELL + TAG_CELL_NAME;
        size += PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE;
        size += column.getNameRawData().length;
        size += 1; // TAG_CELL_VALUE
        size += computePrimaryKeyValue(column.getValue());
        size += 2; // TAG_CELL_CHECKSUM + checksum
        return size;
    }

    // Bytes Data: TAG_ROW_PK + [primary key columns]
    public static final int computePrimaryKey(PrimaryKey primaryKey) throws IOException {
        int size = 1; // TAG_ROW_PK
        for (PrimaryKeyColumn column : primaryKey.getPrimaryKeyColumns()) {
            size += computePrimaryKeyColumn(column);
        }
        return size;
    }

    public static final int computePrimaryKeyWithHeader(PrimaryKey primaryKey) throws IOException {
        int size = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE; // Header
        size += computePrimaryKey(primaryKey);
        size += 2; // TAG_ROW_CHECKSUM + checksum
        return size;
    }

    public static void writePrimaryKeyValue(PrimaryKeyValue value, PlainBufferOutputStream output) throws IOException {
        if (value.isInfMin()) {
            output.writeRawLittleEndian32(1);
            output.writeRawByte(PlainBufferConsts.VT_INF_MIN);
            return;
        }

        if (value.isInfMax()) {
            output.writeRawLittleEndian32(1);
            output.writeRawByte(PlainBufferConsts.VT_INF_MAX);
            return;
        }
        
        if (value.isPlaceHolderForAutoIncr()) {
            output.writeRawLittleEndian32(1);
            output.writeRawByte(PlainBufferConsts.VT_AUTO_INCREMENT);
            return;
        }

        switch (value.getType()) {
            case STRING: {
                byte[] rawData = value.asStringInBytes();
                final int prefixLength = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE + 1; // length + type + length
                output.writeRawLittleEndian32(prefixLength + rawData.length); // length + type + value
                output.writeRawByte(PlainBufferConsts.VT_STRING);
                output.writeRawLittleEndian32(rawData.length);
                output.writeBytes(rawData);
                break;
            }
            case INTEGER: {
                output.writeRawLittleEndian32(1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE);
                output.writeRawByte(PlainBufferConsts.VT_INTEGER);
                output.writeRawLittleEndian64(value.asLong());
                break;
            }
            case BINARY: {
                byte[] rawData = value.asBinary();
                final int prefixLength = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE + 1; // length + type + length
                output.writeRawLittleEndian32(prefixLength + rawData.length); // length + type + value
                output.writeRawByte(PlainBufferConsts.VT_BLOB);
                output.writeRawLittleEndian32(rawData.length);
                output.writeBytes(rawData);
                break;
            }
            case DATETIME: {
                output.writeRawLittleEndian32(1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE);
                output.writeRawByte(PlainBufferConsts.VT_DATETIME);
                output.writeRawLittleEndian64(ValueUtil.parseDateTimeToMicroTimestamp(value.asDateTime()));
                break;
            }
            default:
                throw new IOException("Bug: unsupported primary key type: " + value.getType());
        }
    }

    public static void writePrimaryKeyColumn(PrimaryKeyColumn column, PlainBufferOutputStream output, byte checksum) throws IOException {
        output.writeRawByte(PlainBufferConsts.TAG_CELL);
        output.writeRawByte(PlainBufferConsts.TAG_CELL_NAME);
        byte[] rawData = column.getNameRawData();
        output.writeRawLittleEndian32(rawData.length);
        output.writeBytes(rawData);
        output.writeRawByte(PlainBufferConsts.TAG_CELL_VALUE);
        writePrimaryKeyValue(column.getValue(), output);
        output.writeRawByte(PlainBufferConsts.TAG_CELL_CHECKSUM);
        output.writeRawByte(checksum);
    }

    public static byte[] buildPrimaryKeyWithHeader(PrimaryKey primaryKey) throws IOException {
        int size = computePrimaryKeyWithHeader(primaryKey);
        PlainBufferOutputStream output = new PlainBufferOutputStream(size);
        output.writeRawLittleEndian32(PlainBufferConsts.HEADER);
        output.writeRawByte(PlainBufferConsts.TAG_ROW_PK);

        byte rowChecksum = (byte)0x0, cellChecksum;

        for (PrimaryKeyColumn column : primaryKey.getPrimaryKeyColumns()) {
            cellChecksum = PlainBufferCrc8.crc8((byte)0x0, column.getNameRawData());
            cellChecksum = column.getValue().getChecksum(cellChecksum);
            writePrimaryKeyColumn(column, output, cellChecksum);
            rowChecksum = PlainBufferCrc8.crc8(rowChecksum, cellChecksum);
        }

        // 没有deleteMarker, 要与0x0做crc.
        rowChecksum = PlainBufferCrc8.crc8(rowChecksum, (byte)0x0);

        output.writeRawByte(PlainBufferConsts.TAG_ROW_CHECKSUM);
        output.writeRawByte(rowChecksum);

        if (!output.isFull()) {
            throw new IOException("Bug: serialize primary key failed.");
        }
        return output.getBuffer();
    }

    public static byte[] buildRowPutChangeWithHeader(RowPutChange rowChange) throws IOException {

        List<PlainBufferCell> pkCells = new ArrayList<PlainBufferCell>();
        for (PrimaryKeyColumn column : rowChange.getPrimaryKey().getPrimaryKeyColumns()) {
            pkCells.add(PlainBufferConversion.toPlainBufferCell(column));
        }
        List<PlainBufferCell> cells = new ArrayList<PlainBufferCell>();
        for (Column column: rowChange.getColumnsToPut()) {
            cells.add(PlainBufferConversion.toPlainBufferCell(column, false, false, false, (byte)0x0));
        }

        PlainBufferRow row = new PlainBufferRow(pkCells, cells, false);

        int size = computePlainBufferRowWithHeader(row);
        PlainBufferOutputStream output = new PlainBufferOutputStream(size);
        PlainBufferCodedOutputStream codedOutput = new PlainBufferCodedOutputStream(output);
        codedOutput.writeRowWithHeader(row);
        if (!output.isFull()) {
            throw new IOException("Bug: serialize row put change failed.");
        }
        return output.getBuffer();
    }

    public static byte[] buildRowUpdateChangeWithHeader(RowUpdateChange rowChange) throws IOException {
        List<PlainBufferCell> pkCells = new ArrayList<PlainBufferCell>();
        for (PrimaryKeyColumn column : rowChange.getPrimaryKey().getPrimaryKeyColumns()) {
            pkCells.add(PlainBufferConversion.toPlainBufferCell(column));
        }
        List<PlainBufferCell> cells = new ArrayList<PlainBufferCell>();
        if (!rowChange.getColumnsToUpdate().isEmpty()) {
            for (Pair<Column, RowUpdateChange.Type> column : rowChange.getColumnsToUpdate()) {
                switch (column.getSecond()) {
                    case PUT:
                        cells.add(PlainBufferConversion.toPlainBufferCell(column.getFirst(), false, false, false, (byte) 0x0));
                        break;
                    case DELETE:
                        cells.add(PlainBufferConversion.toPlainBufferCell(column.getFirst(), true, false, true, PlainBufferConsts.DELETE_ONE_VERSION));
                        break;
                    case DELETE_ALL:
                        cells.add(PlainBufferConversion.toPlainBufferCell(column.getFirst(), true, true, true, PlainBufferConsts.DELETE_ALL_VERSION));
                        break;
                    case INCREMENT:
                        cells.add(PlainBufferConversion.toPlainBufferCell(column.getFirst(), false, true, true, PlainBufferConsts.INCREMENT));
                    break;
                }
            }
        }

        PlainBufferRow row = new PlainBufferRow(pkCells, cells, false);

        int size = computePlainBufferRowWithHeader(row);
        PlainBufferOutputStream output = new PlainBufferOutputStream(size);
        PlainBufferCodedOutputStream codedOutput = new PlainBufferCodedOutputStream(output);
        codedOutput.writeRowWithHeader(row);

        if (!output.isFull()) {
            throw new IOException("Bug: serialize row update change failed.");
        }
        return output.getBuffer();
    }

    public static byte[] buildRowDeleteChangeWithHeader(RowDeleteChange rowChange) throws IOException {
        List<PlainBufferCell> pkCells = new ArrayList<PlainBufferCell>();
        for (PrimaryKeyColumn column : rowChange.getPrimaryKey().getPrimaryKeyColumns()) {
            pkCells.add(PlainBufferConversion.toPlainBufferCell(column));
        }
        List<PlainBufferCell> cells = new ArrayList<PlainBufferCell>();

        PlainBufferRow row = new PlainBufferRow(pkCells, cells, true);

        int size = computePlainBufferRowWithHeader(row);
        PlainBufferOutputStream output = new PlainBufferOutputStream(size);
        PlainBufferCodedOutputStream codedOutput = new PlainBufferCodedOutputStream(output);
        codedOutput.writeRowWithHeader(row);

        if (!output.isFull()) {
            throw new IOException("Bug: serialize row delete change failed.");
        }
        return output.getBuffer();
    }

}
