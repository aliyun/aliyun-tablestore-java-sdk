package com.alicloud.openservices.tablestore.core.protocol;

import java.io.IOException;
import java.util.List;

public class PlainBufferRow {
    private List<PlainBufferCell> primaryKey;

    private List<PlainBufferCell> cells;

    private boolean hasDeleteMarker = false;

    private byte checksum;
    private boolean hasChecksum = false;
    private PlainBufferExtension extension;

    public PlainBufferRow(List<PlainBufferCell> primaryKey, List<PlainBufferCell> cells, boolean hasDeleteMarker) {
        this.primaryKey = primaryKey;
        this.cells = cells;
        this.hasDeleteMarker = hasDeleteMarker;
        this.extension = new PlainBufferExtension();
    }

    public List<PlainBufferCell> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(List<PlainBufferCell> primaryKey) {
        this.primaryKey = primaryKey;
        this.hasChecksum = false;
    }

    public List<PlainBufferCell> getCells() {
        return cells;
    }

    public void addCell(PlainBufferCell cell) {
        this.cells.add(cell);
        this.hasChecksum = false;
    }

    public boolean hasCells() {
        return !this.cells.isEmpty();
    }

    public boolean hasDeleteMarker() {
        return hasDeleteMarker;
    }

    public void setHasDeleteMarker(boolean hasDeleteMarker) {
        this.hasDeleteMarker = hasDeleteMarker;
        this.hasChecksum = false;
    }

    public PlainBufferExtension getExtension() {
        return  extension;
    }

    public void setExtension(PlainBufferExtension extension) {
        this.extension = extension;
    }

    public boolean hasExtension() {
        return extension.hasSeq();
    }

    /**
     * Will automatically calculate the current checksum and return it. When there is no data change, 
     * the checksum will be cached in the object to reduce unnecessary calculations.
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
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PrimaryKey: ").append(primaryKey);
        sb.append("Cells: ");
        for (PlainBufferCell cell : cells) {
            sb.append("[").append(cell).append("]");
        }
        sb.append(" HasDeleteMarker: " + hasDeleteMarker());
        if (hasExtension()) {
            sb.append(" Extension: {");
            sb.append(getExtension());
            sb.append("}");
        }
        return sb.toString();
    }
}
