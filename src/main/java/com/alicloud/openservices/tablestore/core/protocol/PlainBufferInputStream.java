package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.core.utils.BinaryUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PlainBufferInputStream {

    private ByteBuffer buffer;
    private int lastTag;

    public PlainBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
        this.lastTag = 0;
    }

    public PlainBufferInputStream(byte[] buffer) {
        this(ByteBuffer.wrap(buffer));
    }

    public boolean isAtEnd() {
        return !buffer.hasRemaining();
    }

    public int readTag() throws IOException {
        if (isAtEnd()) {
            lastTag = 0;
            return 0;
        }

        lastTag = readRawByte();
        return lastTag;
    }

    public boolean checkLastTagWas(int tag) {
        return lastTag == tag;
    }

    public int getLastTag() {
        return lastTag;
    }

    public byte readRawByte() throws IOException {
        if (isAtEnd()) {
            throw new IOException(PlainBufferConsts.READ_ROW_BYTE_EOF);
        }
        return buffer.get();
    }

    public long readRawLittleEndian64() throws IOException {
        final byte b1 = readRawByte();
        final byte b2 = readRawByte();
        final byte b3 = readRawByte();
        final byte b4 = readRawByte();
        final byte b5 = readRawByte();
        final byte b6 = readRawByte();
        final byte b7 = readRawByte();
        final byte b8 = readRawByte();
        return (((long) b1 & 0xff)) |
                (((long) b2 & 0xff) << 8) |
                (((long) b3 & 0xff) << 16) |
                (((long) b4 & 0xff) << 24) |
                (((long) b5 & 0xff) << 32) |
                (((long) b6 & 0xff) << 40) |
                (((long) b7 & 0xff) << 48) |
                (((long) b8 & 0xff) << 56);
    }

    public int readRawLittleEndian32() throws IOException {
        final byte b1 = readRawByte();
        final byte b2 = readRawByte();
        final byte b3 = readRawByte();
        final byte b4 = readRawByte();
        return (((int) b1 & 0xff)) |
                (((int) b2 & 0xff) << 8) |
                (((int) b3 & 0xff) << 16) |
                (((int) b4 & 0xff) << 24);
    }

    public boolean readBoolean() throws IOException {
        return readRawByte() != 0;
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readRawLittleEndian64());
    }

    public int readUInt32() throws IOException {
        return readRawLittleEndian32();
    }

    public long readInt64() throws IOException {
        return readRawLittleEndian64();
    }

    public byte[] readBytes(int size) throws IOException {
        if (buffer.remaining() < size) {
            throw new IOException(PlainBufferConsts.READ_BYTE_EOF);
        }

        byte[] result = new byte[size];
        buffer.get(result, 0, size);
        return result;
    }

    public String readUTFString(int size) throws IOException {
        return new String(readBytes(size), Constants.UTF8_ENCODING);
    }

    @Override
    public String toString() {
        return BinaryUtil.toString(this.buffer);
    }
}
