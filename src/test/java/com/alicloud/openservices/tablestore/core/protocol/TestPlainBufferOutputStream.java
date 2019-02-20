package com.alicloud.openservices.tablestore.core.protocol;

import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.*;

public class TestPlainBufferOutputStream {

    @Test
    public void testRemainCalculation() throws Exception {
        Random random = new Random(System.currentTimeMillis());

        int capacity = 10000;
        PlainBufferOutputStream output = new PlainBufferOutputStream(capacity);

        // write double
        capacity -= 8;
        output.writeDouble(0.11);
        assertEquals(capacity, output.remain());
        assertTrue(!output.isFull());

        // write bytes
        for (int i = 0; i < 10; i++) {
            int l = random.nextInt(100);
            capacity -= l;
            byte[] bs = new byte[l];
            output.writeBytes(bs);
            assertEquals(capacity, output.remain());
            assertTrue(!output.isFull());
        }

        // write boolean
        for (int i = 0; i < 100; i++) {
            output.writeBoolean(i % 2 == 0);
            capacity -= 1;
            assertEquals(capacity, output.remain());
            assertTrue(!output.isFull());
        }

        // write raw byte
        for (int i = 0; i < 100; i++) {
            output.writeRawByte((byte) i);
            capacity -= 1;
            assertEquals(capacity, output.remain());
            assertTrue(!output.isFull());
        }

        // write fixed int32
        for (int i = 0; i < 100; i++) {
            output.writeRawLittleEndian32(i * 19);
            capacity -= 4;
            assertEquals(capacity, output.remain());
            assertTrue(!output.isFull());
        }

        // write fixed int64
        for (int i = 0; i < 100; i++) {
            output.writeRawLittleEndian64(i * 19);
            capacity -= 8;
            assertEquals(capacity, output.remain());
            assertTrue(!output.isFull());
        }

        // fill full
        byte[] bs = new byte[capacity];
        output.writeBytes(bs);
        assertEquals(0, output.remain());
        assertTrue(output.isFull());
    }

    @Test
    public void testBufferOverflow() throws Exception {
        int capacity = 100;
        PlainBufferOutputStream output = new PlainBufferOutputStream(capacity);

        // write (fixed 4 bytes int) * 10 + (fixed 8 bytes int) * 5 + 10 bytes
        for (int i = 0; i < 10; i++) {
            output.writeRawLittleEndian32(i);
        }
        for (int i = 0; i < 5; i++) {
            output.writeRawLittleEndian64(i);
        }
        output.writeBytes(new byte[10]);

        // remain 10 bytes
        assertEquals(10, output.remain());
        assertTrue(!output.isFull());

        // fill overflow
        try {
            output.writeBytes(new byte[11]);
            fail();
        } catch (IOException e) {

        }
    }
}
