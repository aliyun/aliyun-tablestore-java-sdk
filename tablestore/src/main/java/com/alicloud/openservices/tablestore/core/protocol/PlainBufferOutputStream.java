package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.io.IOException;

public class PlainBufferOutputStream {

    private byte[] buffer;
    private int capacity;
    private int pos;

    public PlainBufferOutputStream(int capacity) {
        Preconditions.checkArgument(capacity > 0, "The capacity of output stream must be greater than 0.");
        buffer = new byte[capacity];
        this.capacity = capacity;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public boolean isFull() {
        return pos == capacity;
    }

    public int count() {
        return pos;
    }

    public int remain() {
        return capacity - pos;
    }

    public void clear() {
        this.pos = 0;
    }

    public void writeRawByte(final byte value) throws IOException {
        if (pos == capacity) {
            throw new IOException("The buffer is full.");
        }
        buffer[pos++] = value;
    }

    public void writeRawByte(final int value) throws IOException {
        writeRawByte((byte) value);
    }

    /**
     * Write a little-endian 32-bit integer.
     */
    public void writeRawLittleEndian32(final int value) throws IOException {
        writeRawByte((value) & 0xFF);
        writeRawByte((value >> 8) & 0xFF);
        writeRawByte((value >> 16) & 0xFF);
        writeRawByte((value >> 24) & 0xFF);
    }

    /**
     * Write a little-endian 64-bit integer.
     */
    public void writeRawLittleEndian64(final long value) throws IOException {
        writeRawByte((int) (value) & 0xFF);
        writeRawByte((int) (value >> 8) & 0xFF);
        writeRawByte((int) (value >> 16) & 0xFF);
        writeRawByte((int) (value >> 24) & 0xFF);
        writeRawByte((int) (value >> 32) & 0xFF);
        writeRawByte((int) (value >> 40) & 0xFF);
        writeRawByte((int) (value >> 48) & 0xFF);
        writeRawByte((int) (value >> 56) & 0xFF);
    }

    public void writeDouble(final double value) throws IOException {
        writeRawLittleEndian64(Double.doubleToRawLongBits(value));
    }

    public void writeBoolean(final boolean value) throws IOException {
        writeRawByte(value ? 1 : 0);
    }

    public void writeBytes(final byte[] bytes) throws IOException {
        if (pos + bytes.length > capacity) {
            throw new IOException("The buffer is full.");
        }
        System.arraycopy(bytes, 0, buffer, pos, bytes.length);
        pos += bytes.length;
    }

    public static final int LITTLE_ENDIAN_32_SIZE = 4;
    public static final int LITTLE_ENDIAN_64_SIZE = 8;
}
