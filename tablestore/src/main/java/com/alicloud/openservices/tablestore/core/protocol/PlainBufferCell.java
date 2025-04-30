package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.core.utils.Bytes;

import java.io.IOException;

public class PlainBufferCell {

    private String cellName;
    private byte[] nameRawData;
    private boolean hasCellName = false;

    private ColumnValue cellValue;
    private boolean hasCellValue = false;
    
    private boolean isPk = false;
    private PrimaryKeyValue pkCellValue;

    private byte cellType;
    private boolean hasCellType = false;

    private long cellTimestamp;
    private boolean hasCellTimestamp = false;

    private byte checksum;
    private boolean hasChecksum = false;

    public PlainBufferCell() {

    }

    public String getCellName() {
        return cellName;
    }

    public byte[] getNameRawData() {
        if (nameRawData == null) {
            nameRawData = Bytes.toBytes(cellName);
        }
        return nameRawData;
    }

    public void setCellName(String cellName) {
        this.cellName = cellName;
        this.hasCellName = true;
        nameRawData = null;
        this.hasChecksum = false;
    }

    public boolean hasCellName() {
        return hasCellName;
    }

    public ColumnValue getCellValue() {
        return cellValue;
    }

    public void setCellValue(ColumnValue cellValue) {
        this.cellValue = cellValue;
        this.hasCellValue = true;
        this.hasChecksum = false;
    }

    public boolean hasCellValue() {
        return hasCellValue;
    }

    public byte getCellType() {
        return cellType;
    }

    public void setCellType(byte cellType) {
        this.cellType = cellType;
        this.hasCellType = true;
        this.hasChecksum = false;
    }

    public boolean hasCellType() {
        return hasCellType;
    }

    public long getCellTimestamp() {
        return cellTimestamp;
    }

    public void setCellTimestamp(long cellTimestamp) {
        this.cellTimestamp = cellTimestamp;
        this.hasCellTimestamp = true;
        this.hasChecksum = false;
    }

    public boolean hasCellTimestamp() {
        return hasCellTimestamp;
    }

    /**
     * Automatically calculates the current checksum and returns it. When there is no data change, 
     * the checksum is cached in the object to reduce unnecessary calculations.
     * @return
     * @throws IOException
     */
    public byte getChecksum() throws IOException {
        if (!this.hasChecksum) {
            generateChecksum();
        }
        return this.checksum;
    }

    private void generateChecksum() throws IOException {
        this.checksum = PlainBufferCrc8.getChecksum((byte)0x0, this);
        this.hasChecksum = true;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || ! (o instanceof PlainBufferCell)) {
            return false;
        }

        try {
            if (getChecksum() != ((PlainBufferCell) o).getChecksum()) {
                return false;
            }
        } catch (IOException e) {
            throw new ClientException("Error when getChecksum.", e);
        }

        if ((hasCellName() != ((PlainBufferCell) o).hasCellName())
                || (hasCellName() && !getCellName().equals(((PlainBufferCell) o).getCellName()))) {
            return false;
        };
        if ((hasCellValue() != ((PlainBufferCell) o).hasCellValue())
                || (hasCellValue() && !getCellValue().equals(((PlainBufferCell) o).getCellValue()))) {
            return false;
        }
        if ((isPk() != ((PlainBufferCell) o).isPk())
                || (isPk() && !getPkCellValue().equals(((PlainBufferCell) o).getPkCellValue()))) {
            return false;
        }
        if ((hasCellType() != ((PlainBufferCell) o).hasCellType())
                || (hasCellType() && (getCellType() != ((PlainBufferCell) o).getCellType()))) {
            return false;
        }
        if ((hasCellTimestamp() != ((PlainBufferCell) o).hasCellTimestamp())
                || (hasCellTimestamp() && (getCellTimestamp() != ((PlainBufferCell) o).getCellTimestamp()))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CellName: " + hasCellName() + "|" + cellName);
        sb.append(", CellValue: " + hasCellValue() + "|" + cellValue);
        sb.append(", CellType: " + hasCellType() + "|" + cellType);
        sb.append(", IsPk: " + isPk() + "|" + getPkCellValue());
        sb.append(", CellTimestamp: " + hasCellTimestamp() + "|" + cellTimestamp);
        sb.append(", Checksum: " + this.hasChecksum + "|" + checksum);
        return sb.toString();
    }

    public boolean isPk() {
	    return isPk;
    }

    public PrimaryKeyValue getPkCellValue() {
	    return pkCellValue;
    }

    public void setPkCellValue(PrimaryKeyValue pkCellValue) {
        this.pkCellValue = pkCellValue;
        this.hasCellValue = true;
        this.hasChecksum = false;
        this.isPk = true;
    }
}
