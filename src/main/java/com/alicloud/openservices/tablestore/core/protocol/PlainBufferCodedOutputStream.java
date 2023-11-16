package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.io.IOException;

import static com.alicloud.openservices.tablestore.core.protocol.PlainBufferBuilder.*;
import static com.alicloud.openservices.tablestore.core.protocol.PlainBufferConsts.*;

public class PlainBufferCodedOutputStream {

    private PlainBufferOutputStream output = null;

    public PlainBufferCodedOutputStream(PlainBufferOutputStream output) {
        Preconditions.checkNotNull(output, "The plainBufferOutputStream should not be null.");
        this.output = output;
    }

    public void writeHeader() throws IOException {
        output.writeRawLittleEndian32(HEADER);
    }

    public void writeTag(byte tag) throws IOException {
        output.writeRawByte(tag);
    }

    public void writeCellName(byte[] name) throws IOException {
        writeTag(TAG_CELL_NAME);
        output.writeRawLittleEndian32(name.length);
        output.writeBytes(name);
    }
    
    public void writeCellValue(PrimaryKeyValue value) throws IOException {
        writeTag(TAG_CELL_VALUE);
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
            case DATETIME:{
                output.writeRawLittleEndian32(1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE);
                output.writeRawByte(PlainBufferConsts.VT_DATETIME);
                output.writeRawLittleEndian64(ValueUtil.parseDateTimeToMicroTimestamp(value.asDateTime()));
                break;
            }
            default:
                throw new IOException("Bug: unsupported primary key type: " + value.getType());
        }  	
    }

    public void writeCellValue(ColumnValue value) throws IOException {
        writeTag(TAG_CELL_VALUE);
        switch (value.getType()) {
            case STRING: {
                byte[] rawData = value.asStringInBytes();
                final int prefixLength = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE + 1; // length + type
                output.writeRawLittleEndian32(prefixLength + rawData.length); // length + type + value
                output.writeRawByte(VT_STRING);
                output.writeRawLittleEndian32(rawData.length);
                output.writeBytes(rawData);
                break;
            }
            case INTEGER: {
                output.writeRawLittleEndian32(1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE);
                output.writeRawByte(VT_INTEGER);
                output.writeRawLittleEndian64(value.asLong());
                break;
            }
            case BINARY: {
                byte[] rawData = value.asBinary();
                final int prefixLength = PlainBufferOutputStream.LITTLE_ENDIAN_32_SIZE + 1; // length + type
                output.writeRawLittleEndian32(prefixLength + rawData.length); // length + type
                output.writeRawByte(VT_BLOB);
                output.writeRawLittleEndian32(rawData.length);
                output.writeBytes(rawData);
                break;
            }
            case DOUBLE: {
                output.writeRawLittleEndian32(1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE);
                output.writeRawByte(VT_DOUBLE);
                output.writeDouble(value.asDouble());
                break;
            }
            case BOOLEAN: {
                output.writeRawLittleEndian32(2);
                output.writeRawByte(VT_BOOLEAN);
                output.writeBoolean(value.asBoolean());
                break;
            }
            case DATETIME: {
                output.writeRawLittleEndian32(1 + PlainBufferOutputStream.LITTLE_ENDIAN_64_SIZE);
                output.writeRawByte(VT_DATETIME);
                output.writeRawLittleEndian64(ValueUtil.parseDateTimeToMicroTimestamp(value.asDateTime()));
                break;
            }
            default:
                throw new IOException("Bug: unsupported column type: " + value.getType());
        }
    }

    public void writeCell(PlainBufferCell cell) throws IOException {
        writeTag(TAG_CELL);
        if (cell.hasCellName()) {
            writeCellName(cell.getNameRawData());
        }
        if (cell.hasCellValue()) {
            if (cell.isPk()) {
                writeCellValue(cell.getPkCellValue());
            } else {
                writeCellValue(cell.getCellValue());
            }
        }
        if (cell.hasCellType()) {
            writeTag(TAG_CELL_TYPE);
            output.writeRawByte(cell.getCellType());
        }
        if (cell.hasCellTimestamp()) {
            writeTag(TAG_CELL_TIMESTAMP);
            output.writeRawLittleEndian64(cell.getCellTimestamp());
        }
        writeTag(TAG_CELL_CHECKSUM);
        output.writeRawByte(cell.getChecksum());
    }

    public void writeExtension(PlainBufferExtension extension) throws IOException {
        writeTag(TAG_EXTENSION);
        output.writeRawLittleEndian32(computeSkipLengthForExtensionTag(extension));
        int extensionCount = 0;
        if (extension.hasSeq()) {
            writeSequenceInfo(extension.getSequenceInfo());
            extensionCount++;
        }

        if (extensionCount == 0) {
            throw new IOException("no extension tag is writen.");
        }
    }

    public void writeSequenceInfo(PlainBufferSequenceInfo sequenceInfo) throws IOException {
        writeTag(TAG_SEQ_INFO);
        output.writeRawLittleEndian32(computeSkipLengthForSequenceInfo());
        writeTag(TAG_SEQ_INFO_EPOCH);
        output.writeRawLittleEndian32(sequenceInfo.getEpoch());
        writeTag(TAG_SEQ_INFO_TS);
        output.writeRawLittleEndian64(sequenceInfo.getTimestamp());
        writeTag(TAG_SEQ_INFO_ROW_INDEX);
        output.writeRawLittleEndian32(sequenceInfo.getRowIndex());
    }

    public void writeRow(PlainBufferRow row) throws IOException {
        writeTag(TAG_ROW_PK);
        for (PlainBufferCell cell : row.getPrimaryKey()) {
            writeCell(cell);
        }
        if (!row.getCells().isEmpty()) {
            writeTag(TAG_ROW_DATA);
            for (PlainBufferCell cell : row.getCells()) {
                writeCell(cell);
            }
        }
        if (row.hasDeleteMarker()) {
            writeTag(TAG_DELETE_ROW_MARKER);
        }

        if (row.hasExtension()) {
            writeExtension(row.getExtension());
        }

        writeTag(TAG_ROW_CHECKSUM);
        output.writeRawByte(row.getChecksum());
    }

    public void writeRowWithHeader(PlainBufferRow row) throws IOException {
        writeHeader();
        writeRow(row);
    }

    public void writePrimaryKeyValueWithoutLengthPrefix(PrimaryKeyValue value) throws IOException {
        if (value.isInfMin()) {
            output.writeRawByte(VT_INF_MIN);
            return;
        }

        if (value.isInfMax()) {
            output.writeRawByte(VT_INF_MAX);
            return;
        }
        
        if (value.isPlaceHolderForAutoIncr()) {
            output.writeRawByte(VT_AUTO_INCREMENT);
            return;
        }

        switch (value.getType()) {
            case STRING: {
                byte[] rawData = value.asStringInBytes();
                output.writeRawByte(VT_STRING);
                output.writeRawLittleEndian32(rawData.length);
                output.writeBytes(rawData);
                break;
            }
            case INTEGER: {
                output.writeRawByte(VT_INTEGER);
                output.writeRawLittleEndian64(value.asLong());
                break;
            }
            case BINARY: {
                byte[] rawData = value.asBinary();
                output.writeRawByte(VT_BLOB);
                output.writeRawLittleEndian32(rawData.length);
                output.writeBytes(rawData);
                break;
            }
            case DATETIME: {
                output.writeRawByte(VT_DATETIME);
                output.writeRawLittleEndian64(ValueUtil.parseDateTimeToMicroTimestamp(value.asDateTime()));
                break;
            }
            default:
                throw new IOException("Bug: unsupported primary key type: " + value.getType());
        }
    }


    public void writeColumnValueWithoutLengthPrefix(ColumnValue value) throws IOException {
        switch (value.getType()) {
            case STRING: {
                byte[] rawData = value.asStringInBytes();
                output.writeRawByte(VT_STRING);
                output.writeRawLittleEndian32(rawData.length);
                output.writeBytes(rawData);
                break;
            }
            case INTEGER: {
                output.writeRawByte(VT_INTEGER);
                output.writeRawLittleEndian64(value.asLong());
                break;
            }
            case BINARY: {
                byte[] rawData = value.asBinary();
                output.writeRawByte(VT_BLOB);
                output.writeRawLittleEndian32(rawData.length);
                output.writeBytes(rawData);
                break;
            }
            case DOUBLE: {
                output.writeRawByte(VT_DOUBLE);
                output.writeDouble(value.asDouble());
                break;
            }
            case BOOLEAN: {
                output.writeRawByte(VT_BOOLEAN);
                output.writeBoolean(value.asBoolean());
                break;
            }
            case DATETIME: {
                output.writeRawByte(VT_DATETIME);
                output.writeRawLittleEndian64(ValueUtil.parseDateTimeToMicroTimestamp(value.asDateTime()));
                break;
            }
            default:
                throw new IOException("Bug: unsupported column type: " + value.getType());
        }
    }
}
