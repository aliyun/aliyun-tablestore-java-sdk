package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.utils.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class TestPlainBufferInputStream {

    private static interface CheckValue {
        public void check(PlainBufferInputStream input) throws Exception;
    }

    @Test
    public void testVariousType() throws Exception {
        PlainBufferOutputStream output = new PlainBufferOutputStream(50000);

        List<CheckValue> checkings = new ArrayList<CheckValue>();
        Random random = new Random(System.currentTimeMillis());
        // insert multi elements with various type
        for (int i = 0; i < 1000; i++) {
            int type = Math.abs(random.nextInt()) % 9;
            switch (type) {
                case 0: // varint 32
                {
                    final int value = random.nextInt();
                    output.writeRawLittleEndian32(value);
                    checkings.add(new CheckValue() {
                        @Override
                        public void check(PlainBufferInputStream input) throws Exception {
                            assertEquals(input.readRawLittleEndian32(), value);
                        }
                    });
                    break;
                }
                case 1: // varint 64
                {
                    final long value = random.nextLong();
                    output.writeRawLittleEndian64(value);
                    checkings.add(new CheckValue() {
                        @Override
                        public void check(PlainBufferInputStream input) throws Exception {
                            assertEquals(input.readRawLittleEndian64(), value);
                        }
                    });
                    break;
                }
                case 2: // int32
                {
                    final int value = random.nextInt();
                    output.writeRawLittleEndian32(value);
                    checkings.add(new CheckValue() {
                        @Override
                        public void check(PlainBufferInputStream input) throws Exception {
                            assertEquals(input.readRawLittleEndian32(), value);
                        }
                    });
                    break;
                }
                case 3: // int64
                {
                    final long value = random.nextLong();
                    output.writeRawLittleEndian64(value);
                    checkings.add(new CheckValue() {
                        @Override
                        public void check(PlainBufferInputStream input) throws Exception {
                            assertEquals(input.readRawLittleEndian64(), value);
                        }
                    });
                    break;
                }
                case 4: // double
                {
                    final long value = random.nextLong();
                    output.writeRawLittleEndian64(value);
                    checkings.add(new CheckValue() {
                        @Override
                        public void check(PlainBufferInputStream input) throws Exception {
                            assertEquals(input.readRawLittleEndian64(), value);
                        }
                    });
                    break;
                }
                case 5: // boolean
                {
                    final boolean value = i % 2 == 0;
                    output.writeBoolean(value);
                    checkings.add(new CheckValue() {
                        @Override
                        public void check(PlainBufferInputStream input) throws Exception {
                            assertEquals(input.readBoolean(), value);
                        }
                    });
                    break;
                }
                case 6: // byte
                {
                    final byte value = (byte)random.nextInt();
                    output.writeRawByte(value);
                    checkings.add(new CheckValue() {
                        @Override
                        public void check(PlainBufferInputStream input) throws Exception {
                            assertEquals(input.readRawByte(), value);
                        }
                    });
                    break;
                }
                case 7: // String
                {
                    final String str = "Hello World";
                    final byte[] value = Bytes.toBytes(str);
                    output.writeRawLittleEndian32(value.length);
                    output.writeBytes(value);
                    checkings.add(new CheckValue() {
                        @Override
                        public void check(PlainBufferInputStream input) throws Exception {
                            assertEquals(input.readRawLittleEndian32(), value.length);
                            assertEquals(input.readUTFString(value.length), str);
                        }
                    });
                    break;
                }
                case 8: // bytes
                {
                    final byte[] value = new byte[random.nextInt(100)];
                    random.nextBytes(value);
                    output.writeRawLittleEndian32(value.length);
                    output.writeBytes(value);
                    checkings.add(new CheckValue() {
                        @Override
                        public void check(PlainBufferInputStream input) throws Exception {
                            assertEquals(input.readRawLittleEndian32(), value.length);
                            assertArrayEquals(input.readBytes(value.length), value);
                        }
                    });
                    break;
                }
            }
        }

        byte[] buffer = Arrays.copyOf(output.getBuffer(), output.count());
        PlainBufferInputStream input = new PlainBufferInputStream(buffer);
        for (CheckValue c : checkings) {
            c.check(input);
        }

        assertTrue(input.isAtEnd());
    }


    @Test
    public void testReadTag() throws Exception {
        byte[] data = new byte[] {PlainBufferConsts.TAG_CELL, PlainBufferConsts.TAG_CELL_NAME, PlainBufferConsts.TAG_CELL_TYPE,
                PlainBufferConsts.TAG_CELL_TIMESTAMP, PlainBufferConsts.TAG_CELL_VALUE, PlainBufferConsts.TAG_ROW_DATA,
                PlainBufferConsts.TAG_ROW_PK};

        PlainBufferInputStream input = new PlainBufferInputStream(data);
        for (byte tag : data) {
            assertEquals(tag, input.readTag());
            assertTrue(input.checkLastTagWas(tag));
        }

        assertTrue(input.isAtEnd());
        assertEquals(0, input.readTag());
    }

    @Test
    public void testReadOverflow() {
        int capacity = 100;
        byte[] data = new byte[capacity];
        PlainBufferInputStream input = new PlainBufferInputStream(data);

        try {
            input.readBytes(capacity + 1);
            fail();
        } catch (IOException e) {
        }

        input = new PlainBufferInputStream(new byte[0]);
        try {
            input.readRawLittleEndian32();
            fail();
        } catch (IOException e) {

        }
        try {
            input.readRawByte();
            fail();
        } catch (IOException e) {

        }
        try {
            input.readBoolean();
            fail();
        } catch (IOException e) {

        }
        try {
            input.readDouble();
            fail();
        } catch (IOException e) {

        }
        try {
            input.readInt64();
            fail();
        } catch (IOException e) {

        }
        try {
            input.readRawLittleEndian64();
            fail();
        } catch (IOException e) {

        }
    }
}
