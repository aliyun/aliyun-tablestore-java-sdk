package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi.ActionType;
import com.alicloud.openservices.tablestore.model.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.alicloud.openservices.tablestore.core.Constants.TIMESERIES_HIDDEN_PK;

public class PlainBufferConversion {
    public static Row toRow(PlainBufferRow plainBufferRow) throws IOException {
        return toRow(plainBufferRow, true);
    }

    public static Row toRow(PlainBufferRow plainBufferRow, boolean needSortColumns) throws IOException {
        if (plainBufferRow.hasDeleteMarker()) {
            throw new IOException("Row could not has delete marker: " + plainBufferRow);
        }

        if (plainBufferRow.getPrimaryKey() == null) {
            throw new IOException("Row has no primary key: " + plainBufferRow);
        }

        List<Column> columns = new ArrayList<Column>(plainBufferRow.getCells().size());

        for (PlainBufferCell cell : plainBufferRow.getCells()) {
            columns.add(toColumn(cell));
        }

        return new Row(toPrimaryKey(plainBufferRow.getPrimaryKey()), columns, needSortColumns);
    }

    public static Column toColumn(PlainBufferCell cell) throws IOException {
        if (!cell.hasCellName() || !cell.hasCellValue()) {
            throw new IOException("The cell has no name or value: " + cell);
        }

        if (cell.hasCellType() &&
                cell.getCellType() != PlainBufferConsts.INCREMENT) {
            throw new IOException("The cell should not has type: " + cell);
        }

        if (cell.hasCellTimestamp()) {
            return new Column(cell.getCellName(), cell.getCellValue(), cell.getCellTimestamp());
        } else {
            return new Column(cell.getCellName(), cell.getCellValue());
        }
    }

    public static RecordColumn toRecordColumn(PlainBufferCell cell) throws IOException {
        if (!cell.hasCellName()) {
            throw new IOException("The cell has no name:" + cell);
        }

        ColumnValue value = ColumnValue.INTERNAL_NULL_VALUE;
        if (cell.hasCellValue()) {
            value = cell.getCellValue();
        }

        Column column = null;

        if (cell.hasCellTimestamp()) {
            column = new Column(cell.getCellName(), value, cell.getCellTimestamp());
        } else {
            column = new Column(cell.getCellName(), value);
        }

        RecordColumn.ColumnType columnType = RecordColumn.ColumnType.PUT;

        if (!cell.hasCellType()) {
            if (!cell.hasCellValue() || !cell.hasCellTimestamp()) {
                throw new IOException("The cell should have both value and timestamp: " + cell);
            }
        } else {
            switch (cell.getCellType()) {
                case PlainBufferConsts.DELETE_ONE_VERSION:
                    if (cell.hasCellValue() || !cell.hasCellTimestamp()) {
                        throw new IOException(
                                "The cell with type DELETE_ONE_VERSION should not have value but should have timestamp: "
                                        + cell);
                    }
                    columnType = RecordColumn.ColumnType.DELETE_ONE_VERSION;
                    break;
                case PlainBufferConsts.DELETE_ALL_VERSION:
                    if (cell.hasCellValue() || cell.hasCellTimestamp()) {
                        throw new IOException(
                                "The cell with type DELETE_ALL_VERSION should not have value and timestamp: " + cell);
                    }
                    columnType = RecordColumn.ColumnType.DELETE_ALL_VERSION;
                    break;
                default:
                    throw new IOException("Unknown cell type:" + cell.getCellType());
            }
        }

        return new RecordColumn(column, columnType);
    }

    public static RecordColumn toTimeseriesRecordColumn(PlainBufferCell cell) throws IOException {
        RecordColumn recordColumn = toRecordColumn(cell);
        Column column = recordColumn.getColumn();
        Column timeseriesFormatColumn;

        if (column.hasSetTimestamp()){
            timeseriesFormatColumn = new Column(column.getName().split(":")[0], column.getValue(), column.getTimestamp());
        } else {
            timeseriesFormatColumn = new Column(column.getName().split(":")[0], column.getValue());
        }
        recordColumn.setColumn(timeseriesFormatColumn);

        return recordColumn;
    }

    public static PrimaryKey toPrimaryKey(List<PlainBufferCell> pkCells) throws IOException {
        List<PrimaryKeyColumn> primaryKeyColumns = new ArrayList<PrimaryKeyColumn>();
        for (PlainBufferCell cell : pkCells) {
            primaryKeyColumns.add(
                    new PrimaryKeyColumn(cell.getCellName(), cell.getPkCellValue()));
        }
        return new PrimaryKey(primaryKeyColumns);
    }

    public static PrimaryKey toTimeseriesPrimaryKey(List<PlainBufferCell> pkCells) throws IOException {
        List<PrimaryKeyColumn> primaryKeyColumns = new ArrayList<>();
        for (int i = 0; i < pkCells.size(); i++) {
            if (i == 0 && pkCells.get(i).getCellName().equals(TIMESERIES_HIDDEN_PK)) {
                // ignore _#h column
                continue;
            }
            primaryKeyColumns.add(new PrimaryKeyColumn(pkCells.get(i).getCellName(), pkCells.get(i).getPkCellValue()));
        }
        return new PrimaryKey(primaryKeyColumns);
    }

    public static PlainBufferCell toPlainBufferCell(PrimaryKeyColumn primaryKeyColumn) throws IOException {
        PlainBufferCell cell = new PlainBufferCell();
        cell.setCellName(primaryKeyColumn.getName());
        cell.setPkCellValue(primaryKeyColumn.getValue());
        return cell;
    }

    public static PlainBufferCell toPlainBufferCell(Column column, boolean ignoreValue, boolean ignoreTs,
                                                    boolean setType, byte type) throws IOException {
        PlainBufferCell cell = new PlainBufferCell();
        cell.setCellName(column.getName());
        if (!ignoreValue) {
            cell.setCellValue(column.getValue());
        }
        if (!ignoreTs) {
            if (column.hasSetTimestamp()) {
                cell.setCellTimestamp(column.getTimestamp());
            }
        }
        if (setType) {
            cell.setCellType(type);
        }
        return cell;
    }


    public static StreamRecord toStreamRecord(PlainBufferRow pbRow, PlainBufferRow pbOriginRow, OtsInternalApi.ActionType actionType, boolean parseInTimeseriesDataFormat)
            throws IOException {
        StreamRecord record = new StreamRecord();
        switch (actionType) {
            case PUT_ROW:
                record.setRecordType(StreamRecord.RecordType.PUT);
                break;
            case UPDATE_ROW:
                record.setRecordType(StreamRecord.RecordType.UPDATE);
                break;
            case DELETE_ROW:
                record.setRecordType(StreamRecord.RecordType.DELETE);
                break;
            default:
                throw new IOException("Unknown stream record action type:" + actionType.name());
        }
        if (parseInTimeseriesDataFormat) {
            record.setPrimaryKey(toTimeseriesPrimaryKey(pbRow.getPrimaryKey()));

            List<RecordColumn> columns = new ArrayList<RecordColumn>();
            for (PlainBufferCell cell : pbRow.getCells()) {
                columns.add(toTimeseriesRecordColumn(cell));
            }
            record.setColumns(columns);

            if (pbOriginRow != null) {
                List<RecordColumn> originColumns = new ArrayList<RecordColumn>();
                for (PlainBufferCell cell : pbOriginRow.getCells()) {
                    originColumns.add(toTimeseriesRecordColumn(cell));
                }
                record.setOriginColumns(originColumns);
            }
        } else {
            record.setPrimaryKey(toPrimaryKey(pbRow.getPrimaryKey()));

            List<RecordColumn> columns = new ArrayList<RecordColumn>();
            for (PlainBufferCell cell : pbRow.getCells()) {
                columns.add(toRecordColumn(cell));
            }
            record.setColumns(columns);

            if (pbOriginRow != null) {
                List<RecordColumn> originColumns = new ArrayList<RecordColumn>();
                for (PlainBufferCell cell : pbOriginRow.getCells()) {
                    originColumns.add(toRecordColumn(cell));
                }
                record.setOriginColumns(originColumns);
            }
        }


        int epoch = pbRow.getExtension().getSequenceInfo().getEpoch();
        long ts = pbRow.getExtension().getSequenceInfo().getTimestamp();
        int rowIndex = pbRow.getExtension().getSequenceInfo().getRowIndex();
        RecordSequenceInfo seq = new RecordSequenceInfo(epoch, ts, rowIndex);
        record.setSequenceInfo(seq);
        return record;
    }

    public static StreamRecord toStreamRecord(PlainBufferRow pbRow, PlainBufferRow pbOriginRow, TunnelServiceApi.ActionType actionType, boolean parseInTimeseriesDataFormat)
            throws IOException {
        OtsInternalApi.ActionType atype;
        switch (actionType) {
            case PUT_ROW:
                atype = ActionType.PUT_ROW;
                break;
            case UPDATE_ROW:
                atype = ActionType.UPDATE_ROW;
                break;
            case DELETE_ROW:
                atype = ActionType.DELETE_ROW;
                break;
            default:
                throw new IOException("Unknown stream record action type:" + actionType.name());
        }
        return toStreamRecord(pbRow, pbOriginRow, atype, parseInTimeseriesDataFormat);
    }
}
