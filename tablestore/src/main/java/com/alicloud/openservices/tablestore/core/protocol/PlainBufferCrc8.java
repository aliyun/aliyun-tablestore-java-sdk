package com.alicloud.openservices.tablestore.core.protocol;

import java.io.IOException;

/**
 * Uses the crc-8-ATM standard
 * Polynomial: x^8 + x^2 + x + 1
 *
 */
public class PlainBufferCrc8 {

    private static final int spaceSize = 256;
    private static byte[] crc8Table = new byte[spaceSize];
    private static byte[] crc8Table2 = new byte[spaceSize];
    private static byte[] crc8Table4 = new byte[spaceSize];


    static {
        for (int i = 0; i < crc8Table.length; ++i) {
            byte x = (byte) i;
            for (int j = 8; j > 0; --j) {
                x = (byte) ((x << 1) ^ (((x & 0x80) != 0) ? 0x07 : 0));
            }
            crc8Table[i] = x;
        }
        for (int i = 0; i < crc8Table.length; ++i) {
            crc8Table2[i] = crc8Table[crc8Table[i] & 0xff];
        }
        for (int i = 0; i < crc8Table.length; ++i) {
            crc8Table4[i] = crc8Table2[crc8Table2[i] & 0xff];
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
        return crc8Fast(crc, in);
    }

    public static byte crc8Slow(byte crc, byte[] in) {
        for (int i = 0; i < in.length; i++) {
            crc = crc8(crc, in[i]);
        }
        return crc;
    }

    public static byte crc8Fast(byte crc, byte[] in) {
        byte crc1 = crc;
        byte crc2 = 0;
        byte crc3 = 0;
        byte crc4 = 0;
        int i = 0;
        for (; (i + 8) <= in.length; i+=8) {
            crc1 = crc8(crc1, in[i]);
            crc1 = crc8(crc1, in[i+1]);
            crc2 = crc8((byte)0, in[i+2]);
            crc2 = crc8(crc2, in[i+3]);
            crc3 = crc8((byte)0, in[i+4]);
            crc3 = crc8(crc3, in[i+5]);
            crc4 = crc8((byte)0, in[i+6]);
            crc4 = crc8(crc4, in[i+7]);
            crc1 = (byte) (crc8Table2[crc1 & 0xff] ^ crc2);
            crc3 = (byte) (crc8Table2[crc3 & 0xff] ^ crc4);
            crc1 = (byte) (crc8Table4[crc1 & 0xff] ^ crc3);
        }
        for (; i < in.length; i++) {
            crc1 = crc8(crc1, in[i]);
        }
        return crc1;
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
