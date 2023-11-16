package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.alicloud.openservices.tablestore.core.protocol.PlainBufferConsts.*;

public class PlainBufferCodedInputStream {

    private static Logger logger = LoggerFactory.getLogger(PlainBufferCodedInputStream.class);
    private PlainBufferInputStream input = null;

    public PlainBufferCodedInputStream(PlainBufferInputStream input) {
        Preconditions.checkNotNull(input, "The plainBufferInputStream should not be null.");
        this.input = input;
    }

    public int readTag() throws IOException {
        return input.readTag();
    }

    public boolean checkLastTagWas(int tag) {
        return input.checkLastTagWas(tag);
    }

    public int getLastTag() {
        return input.getLastTag();
    }

    public int readHeader() throws IOException {
        return input.readUInt32();
    }

    public ColumnValue readCellValue() throws IOException {
        if (!checkLastTagWas(TAG_CELL_VALUE)) {
            throw new IOException("Expect TAG_CELL_VALUE but it was " + PlainBufferConsts.printTag(getLastTag()));
        }
        int length = input.readRawLittleEndian32();
        byte type = input.readRawByte();
        ColumnValue columnValue = null;
        switch (type) {
            case VT_INTEGER: {
                columnValue = ColumnValue.fromLong(input.readInt64());
                break;
            }
            case VT_BLOB: {
                columnValue = ColumnValue.fromBinary(input.readBytes(input.readUInt32()));
                break;
            }
            case VT_STRING: {
                columnValue = ColumnValue.fromString(input.readUTFString(input.readUInt32()));
                break;
            }
            case VT_BOOLEAN: {
                columnValue = ColumnValue.fromBoolean(input.readBoolean());
                break;
            }
            case VT_DOUBLE: {
                columnValue = ColumnValue.fromDouble(input.readDouble());
                break;
            }
            case VT_DATETIME:{
                columnValue = ColumnValue.fromDateTime(ValueUtil.parseMicroTimestampToUTCDateTime(input.readInt64()));
                break;
            }
            default:
                throw new IOException("Unsupported column type: " + type);
        }
        readTag();
        return columnValue;
    }

    public void skipRawSize(int length) throws IOException {
          input.readBytes(length);
    }

    public PlainBufferCell readCell() throws IOException {
        if (!checkLastTagWas(TAG_CELL)) {
            throw new IOException("Expect TAG_CELL but it was " + PlainBufferConsts.printTag(getLastTag()));
        }

        PlainBufferCell cell = new PlainBufferCell();

        readTag();
        if (getLastTag() == TAG_CELL_NAME) {
            cell.setCellName(input.readUTFString(input.readRawLittleEndian32()));
            readTag();
        }

        if (getLastTag() == TAG_CELL_VALUE) {
            cell.setCellValue(readCellValue());
        }

        if (getLastTag() == TAG_CELL_TYPE) {
            cell.setCellType(input.readRawByte());
            readTag();
        }

        if (getLastTag() == TAG_CELL_TIMESTAMP) {
            long timestamp = input.readInt64();
            if (timestamp < 0) {
                throw new IOException("The timestamp is negative.");
            }
            cell.setCellTimestamp(timestamp);
            readTag(); // consume next tag as all read function should consume next tag
        }

        if (getLastTag() == TAG_CELL_CHECKSUM) {
            byte checksum = input.readRawByte();
            readTag();
            if (cell.getChecksum() != checksum) {
                logger.error("Checksum mismatch. Cell: " + cell.toString() + ". Checksum: " + checksum + ". PlainBuffer: " + input.toString());
                throw new IOException("Checksum is mismatch.");
            }
        } else {
            throw new IOException("Expect TAG_CELL_CHECKSUM but it was " + PlainBufferConsts.printTag(getLastTag()));
        }

        return cell;
    }

    public List<PlainBufferCell> readRowPK() throws IOException {
        if (!checkLastTagWas(TAG_ROW_PK)) {
            throw new IOException("Expect TAG_ROW_PK but it was " + PlainBufferConsts.printTag(getLastTag()));
        }

        List<PlainBufferCell> primaryKeyColumns = new ArrayList<PlainBufferCell>();
        readTag();
        while (checkLastTagWas(TAG_CELL)) {
            PlainBufferCell cell = readCell();
            primaryKeyColumns.add(cell);
        }
        return primaryKeyColumns;
    }

    public List<PlainBufferCell> readRowData() throws IOException {
        if (!checkLastTagWas(TAG_ROW_DATA)) {
            throw new IOException("Expect TAG_ROW_DATA but it was " + PlainBufferConsts.printTag(getLastTag()));
        }

        List<PlainBufferCell> columns = new ArrayList<PlainBufferCell>();
        readTag();
        while (checkLastTagWas(TAG_CELL)) {
            columns.add(readCell());
        }

        return columns;
    }

    public PlainBufferExtension readExtension() throws IOException {
          PlainBufferExtension extension = new PlainBufferExtension();
          if (checkLastTagWas(TAG_EXTENSION)) {
              input.readUInt32(); // length
              readTag();
              while (isTagInExtension(getLastTag())) {
                  if (checkLastTagWas(TAG_SEQ_INFO)) {
                      extension.setSequenceInfo(readSequenceInfo());
                  } else {
                      int length = input.readRawLittleEndian32();;
                      skipRawSize(length);
                      readTag();
                  }
              }
          }
  
          return extension;
    }
    public PlainBufferRow readRow() throws IOException {
        return readRow(true);
    }

    public PlainBufferRow readRow(Boolean shouldContainsPk)  throws IOException {
        List<PlainBufferCell> columns = new ArrayList<PlainBufferCell>();
        List<PlainBufferCell> primaryKey = new ArrayList<PlainBufferCell>();
        boolean hasDeleteMarker = false;

        if (checkLastTagWas(TAG_ROW_PK)) {
            primaryKey = readRowPK();
            if (shouldContainsPk) {
                if (primaryKey.isEmpty()) {
                    throw new IOException("The primary key of row is empty.");
                }
            }
        }

        if (checkLastTagWas(TAG_ROW_DATA)) {
            columns = readRowData();
        }

        if (checkLastTagWas(TAG_DELETE_ROW_MARKER)) {
            hasDeleteMarker = true;
            readTag();
        }

        PlainBufferRow row = new PlainBufferRow(primaryKey, columns, hasDeleteMarker);

        row.setExtension(readExtension());

        if (checkLastTagWas(TAG_ROW_CHECKSUM)) {
            byte checksum = input.readRawByte();
            readTag();
            if (row.getChecksum() != checksum) {
                logger.error("Checksum mismatch. Row: " + row.toString() + ". Checksum: " + checksum + ". PlainBuffer: " + input.toString());
                throw new IOException("Checksum is mismatch.");
            }
        } else {
            throw new IOException("Expect TAG_ROW_CHECKSUM but it was " + PlainBufferConsts.printTag(getLastTag()));
        }

        return row;
    }

    public PlainBufferSequenceInfo readSequenceInfo() throws IOException {
          if (!checkLastTagWas(TAG_SEQ_INFO)) {
              throw new IOException("Expect TAG_SEQ_INFO but it was " + PlainBufferConsts.printTag(getLastTag()));
          }
          input.readRawLittleEndian32();// length
          readTag();
          PlainBufferSequenceInfo seq = new PlainBufferSequenceInfo();
          if (checkLastTagWas(TAG_SEQ_INFO_EPOCH)) {
              seq.setEpoch(input.readUInt32());
              readTag();
          } else {
              throw new IOException("Expect TAG_SEQ_INFO_EPOCH but it was " + PlainBufferConsts.printTag(getLastTag()));
          }
          if (checkLastTagWas(TAG_SEQ_INFO_TS)) {
              seq.setTimestamp(input.readInt64());
              readTag();
          } else {
              throw new IOException("Expect TAG_SEQ_INFO_TS but it was " + PlainBufferConsts.printTag(getLastTag()));
          }
          if (checkLastTagWas(TAG_SEQ_INFO_ROW_INDEX)) {
              seq.setRowIndex(input.readUInt32());
              readTag();
          } else {
              throw new IOException("Expect TAG_SEQ_INFO_ROW_INDEX but it was " + PlainBufferConsts.printTag(getLastTag()));
          }
  
          return seq;
    }

    public List<PlainBufferRow> readRowsWithHeader() throws IOException {
        List<PlainBufferRow> rows = new ArrayList<PlainBufferRow>();
        if (readHeader() != PlainBufferConsts.HEADER) {
            logger.error("Invalid header from plain buffer: " + input.toString());
            throw new IOException("Invalid header from plain buffer.");
        }

        readTag();
        while (!input.isAtEnd()) {
            PlainBufferRow row = readRow();
            rows.add(row);
        }

        if (!input.isAtEnd()) {
            throw new IOException("");
        }

        return rows;
    }

    public List<PlainBufferRow> readRowsWithoutPk() throws IOException {
        List<PlainBufferRow> rows = new ArrayList<PlainBufferRow>();
        if (readHeader() != PlainBufferConsts.HEADER) {
            logger.error("Invalid header from plain buffer: " + input.toString());
            throw new IOException("Invalid header from plain buffer.");
        }

        readTag();
        while (!input.isAtEnd()) {
            PlainBufferRow row = readRow(false);
            rows.add(row);
        }

        if (!input.isAtEnd()) {
            throw new IOException("");
        }

        return rows;
    }
}
