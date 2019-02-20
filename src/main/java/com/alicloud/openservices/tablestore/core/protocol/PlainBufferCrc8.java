package com.alicloud.openservices.tablestore.core.protocol;

import java.io.IOException;

/**
 * 采用crc-8-ATM规范
 * 多项式: x^8 + x^2 + x + 1
 *
 */
public class PlainBufferCrc8 {

    private static final int spaceSize = 256;
    private static byte[] crc8Table = new byte[spaceSize];

    static {
        for (int i = 0; i < crc8Table.length; ++i) {
            byte x = (byte) i;
            for (int j = 8; j > 0; --j) {
                x = (byte) ((x << 1) ^ (((x & 0x80) != 0) ? 0x07 : 0));
            }
            crc8Table[i] = x;
        }
    }

    public static byte crc8(byte crc, byte in) {
        crc = crc8Table[(crc ^ in) & 0xff];
        return crc;
    }

    public static byte crc8(byte crc, int in) {
        for (int i = 0; i < 4; i++) {
            crc = crc8(crc, (byte)(in & 0xff));
            in >>= 8;
        }
        return crc;
    }

    public static byte crc8(byte crc, long in) {
        for (int i = 0; i < 8; i++) {
            crc = crc8(crc, (byte)(in & 0xff));
            in >>= 8;
        }
        return crc;
    }

    public static byte crc8(byte crc, byte[] in) {
        for (int i = 0; i < in.length; i++) {
            crc = crc8(crc, in[i]);
        }
        return crc;
    }

    public static byte getChecksum(byte crc, PlainBufferCell cell) throws IOException {

        if (cell.hasCellName()) {
            crc = crc8(crc, cell.getNameRawData());
        }

        if (cell.hasCellValue()) {
            if (cell.isPk()) {
                crc = cell.getPkCellValue().getChecksum(crc);
            } else {
                crc = cell.getCellValue().getChecksum(crc);
            }
        }

        if (cell.hasCellTimestamp()) {
            crc = crc8(crc, cell.getCellTimestamp());
        }

        if (cell.hasCellType()) {
            crc = crc8(crc, cell.getCellType());
        }

        return crc;
    }

    public static byte getChecksum(byte crc, PlainBufferRow row) throws IOException {
        for (PlainBufferCell cell : row.getPrimaryKey()) {
            crc = crc8(crc, cell.getChecksum());
        }

        for (PlainBufferCell cell : row.getCells()) {
            crc = crc8(crc, cell.getChecksum());
        }

        byte del = 0;
        if (row.hasDeleteMarker()) {
            del = (byte)0x1;
        }
        crc = crc8(crc, del);

        return crc;
    }
}
