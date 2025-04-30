package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.utils.Bytes;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class PlainBufferCodedInputStreamTest {

    public static class CellCheck {
        public List<ColumnValue> values;
        public List<PrimaryKeyValue> pkValues;
        public PlainBufferOutputStream output;
        public boolean isPK;
        private Random random = new Random(System.currentTimeMillis());

        public CellCheck(boolean isPK) throws IOException {
            this.values = new ArrayList<>();
            this.pkValues = new ArrayList<>();
            this.output = new PlainBufferOutputStream(50000);
            this.isPK = isPK;
        }

        public void writeCellValue(byte type) throws IOException {
            output.writeRawLittleEndian32(0); // length uses 0 since we do not check length here
            output.writeRawByte(type); // write type

            switch (type) { // write value
                case PlainBufferConsts.VT_INTEGER:
                {
                    final long value = random.nextLong();
                    output.writeRawLittleEndian64(value);
                    if (isPK) {
                        pkValues.add(PrimaryKeyValue.fromLong(value));
                    } else {
                        values.add(ColumnValue.fromLong(value));
                    }
                    break;
                }
                case PlainBufferConsts.VT_BLOB:
                {
                    final byte[] value = new byte[random.nextInt(50)];
                    random.nextBytes(value);
                    output.writeRawLittleEndian32(value.length);
                    output.writeBytes(value);
                    if (isPK) {
                        pkValues.add(PrimaryKeyValue.fromBinary(value));
                    } else {
                        values.add(ColumnValue.fromBinary(value));
                    }
                    break;
                }
                case PlainBufferConsts.VT_STRING:
                {
                    final byte[] value = Bytes.toBytes(TEST_STRING_VALUE);
                    output.writeRawLittleEndian32(value.length);
                    output.writeBytes(value);
                    if (isPK) {
                        pkValues.add(PrimaryKeyValue.fromString(TEST_STRING_VALUE));
                    } else {
                        values.add(ColumnValue.fromString(TEST_STRING_VALUE));
                    }
                    break;
                }
                case PlainBufferConsts.VT_BOOLEAN:
                {
                    if (isPK) {
                        throw new IOException("Unsupported pk type: " + type);
                    }
                    final boolean value = random.nextInt(2) == 1;
                    output.writeBoolean(value);
                    values.add(ColumnValue.fromBoolean(value));
                    break;
                }
                case PlainBufferConsts.VT_DOUBLE:
                {
                    if (isPK) {
                        throw new IOException("Unsupported pk type: " + type);
                    }
                    final double value = random.nextDouble();
                    output.writeDouble(value);
                    values.add(ColumnValue.fromDouble(value));
                    break;
                }
                case PlainBufferConsts.VT_INF_MAX:
                {
                    if (!isPK) {
                        throw new IOException("Unsupported column type: " + type);
                    }
                    pkValues.add(PrimaryKeyValue.INF_MAX);
                    break;
                }
                case PlainBufferConsts.VT_INF_MIN:
                {
                    if (!isPK) {
                        throw new IOException("Unsupported column type: " + type);
                    }
                    pkValues.add(PrimaryKeyValue.INF_MIN);
                    break;
                }
            }
            output.writeRawByte(PlainBufferConsts.TAG_CELL_VALUE); // next tag
        }

        public void generateValueCheck() throws IOException {
            // write the CELL_VALUE tag first
            output.writeRawByte(PlainBufferConsts.TAG_CELL_VALUE);

            byte[] supportTypes = isPK ? SUPPORT_PK_VALUE_TYPES : SUPPORT_VALUE_TYPES;
            for (int i=0; i<1000; i++) {
                byte type = supportTypes[i % supportTypes.length];
                this.writeCellValue(type);
            }
        }
    }

    private static byte[] SUPPORT_VALUE_TYPES = {
            PlainBufferConsts.VT_INTEGER,
            PlainBufferConsts.VT_BLOB,
            PlainBufferConsts.VT_STRING,
            PlainBufferConsts.VT_BOOLEAN,
            PlainBufferConsts.VT_DOUBLE
    };
    private static byte[] SUPPORT_PK_VALUE_TYPES = {
            PlainBufferConsts.VT_INTEGER,
            PlainBufferConsts.VT_BLOB,
            PlainBufferConsts.VT_STRING,
            PlainBufferConsts.VT_INF_MAX,
            PlainBufferConsts.VT_INF_MIN
    };
    private final static String TEST_STRING_VALUE = "Test String in plain buffer";

    @Test
    public void testReadTag() throws IOException {
        byte[] data = new byte[] {PlainBufferConsts.TAG_CELL, PlainBufferConsts.TAG_CELL_NAME, PlainBufferConsts.TAG_CELL_TYPE,
                PlainBufferConsts.TAG_CELL_TIMESTAMP, PlainBufferConsts.TAG_CELL_VALUE, PlainBufferConsts.TAG_ROW_DATA,
                PlainBufferConsts.TAG_ROW_PK};

        PlainBufferCodedInputStream codedInput = new PlainBufferCodedInputStream(new PlainBufferInputStream(data));
        for (byte tag : data) {
            assertEquals(tag, codedInput.readTag());
            assertEquals(tag, codedInput.getLastTag());
            assertTrue(codedInput.checkLastTagWas(tag));
        }

        assertEquals(0, codedInput.readTag());
    }

    @Test
    public void testReadHeader() throws IOException {
        // normal test
        {
            byte[] data = new byte[] {(byte)PlainBufferConsts.HEADER, 0, 0, 0};
            PlainBufferCodedInputStream codedInput = new PlainBufferCodedInputStream(new PlainBufferInputStream(data));
            assertEquals(PlainBufferConsts.HEADER, codedInput.readHeader());
        }
        // fail test
        {
            byte[] shortData = new byte[] {0};
            PlainBufferCodedInputStream shortCodedInput = new PlainBufferCodedInputStream(new PlainBufferInputStream(shortData));
            try {
                shortCodedInput.readHeader();
                fail();
            } catch (IOException e) {
                assertEquals(PlainBufferConsts.READ_ROW_BYTE_EOF, e.getMessage());
            } catch (Exception e) {
                fail("Unexpected Exception: " + e.getMessage());
            }
        }
    }

    @Test
    public void testReadCellValue() throws IOException {
        // normal test
        {
            CellCheck cellCheck = new CellCheck(false);
            cellCheck.generateValueCheck();
            byte[] buffer = Arrays.copyOf(cellCheck.output.getBuffer(), cellCheck.output.count());
            PlainBufferCodedInputStream codedInput = new PlainBufferCodedInputStream(new PlainBufferInputStream(buffer));

            // read the CELL_VALUE tag first
            assertEquals(PlainBufferConsts.TAG_CELL_VALUE, codedInput.readTag());
            // check each value
            for (ColumnValue cv : cellCheck.values) {
                assertEquals(cv, codedInput.readCellValue());
            }
        }

        // fail test
        {
            CellCheck cellCheck = new CellCheck(false);
            byte invalidType = PlainBufferConsts.VT_INF_MAX;
            cellCheck.output.writeRawByte(PlainBufferConsts.TAG_CELL_VALUE);
            cellCheck.output.writeRawLittleEndian32(0);
            cellCheck.output.writeRawByte(invalidType);
            cellCheck.output.writeRawByte(PlainBufferConsts.TAG_CELL_VALUE);

            byte[] buffer = Arrays.copyOf(cellCheck.output.getBuffer(), cellCheck.output.count());
            PlainBufferCodedInputStream codedInput = new PlainBufferCodedInputStream(new PlainBufferInputStream(buffer));

            // if you don't read the CELL_VALUE tag first,
            // you will get an IOException
            try {
                codedInput.readCellValue();
                fail();
            } catch (IOException e) {
                assertEquals(
                        "Expect TAG_CELL_VALUE but it was " + PlainBufferConsts.printTag(codedInput.getLastTag()),
                        e.getMessage());
            } catch (Exception e) {
                fail("Unexpected Exception: " + e.getMessage());
            }
            // read the CELL_VALUE tag
            assertEquals(PlainBufferConsts.TAG_CELL_VALUE, codedInput.readTag());

            // invalid type
            try {
                codedInput.readCellValue();
                fail();
            } catch (IOException e) {
                assertEquals(
                        "Unsupported column type: " + invalidType,
                        e.getMessage());
            } catch (Exception e) {
                fail("Unexpected Exception: " + e.getMessage());
            }
        }
    }

    @Test
    public void testReadCellPrimaryKeyValue() throws IOException {
        // normal test
        {
            CellCheck pkCellCheck = new CellCheck(true);
            pkCellCheck.generateValueCheck();
            byte[] buffer = Arrays.copyOf(pkCellCheck.output.getBuffer(), pkCellCheck.output.count());
            PlainBufferCodedInputStream codedInput = new PlainBufferCodedInputStream(new PlainBufferInputStream(buffer));

            // read the CELL_VALUE tag first
            assertEquals(PlainBufferConsts.TAG_CELL_VALUE, codedInput.readTag());
            // check each value
            for (PrimaryKeyValue cv : pkCellCheck.pkValues) {
                assertEquals(cv, codedInput.readCellPrimaryKeyValue());
            }
        }

        // fail test
        {
            CellCheck pkCellCheck = new CellCheck(true);
            byte invalidType = PlainBufferConsts.VT_DOUBLE;
            pkCellCheck.output.writeRawByte(PlainBufferConsts.TAG_CELL_VALUE);
            pkCellCheck.output.writeRawLittleEndian32(0);
            pkCellCheck.output.writeRawByte(invalidType);
            pkCellCheck.output.writeRawByte(PlainBufferConsts.TAG_CELL_VALUE);

            byte[] buffer = Arrays.copyOf(pkCellCheck.output.getBuffer(), pkCellCheck.output.count());
            PlainBufferCodedInputStream codedInput = new PlainBufferCodedInputStream(new PlainBufferInputStream(buffer));

            // if you don't read the CELL_VALUE tag first,
            // you will get an IOException
            try {
                codedInput.readCellPrimaryKeyValue();
                fail();
            } catch (IOException e) {
                assertEquals(
                        "Expect TAG_CELL_VALUE but it was " + PlainBufferConsts.printTag(codedInput.getLastTag()),
                        e.getMessage());
            } catch (Exception e) {
                fail("Unexpected Exception: " + e.getMessage());
            }
            // read the CELL_VALUE tag
            assertEquals(PlainBufferConsts.TAG_CELL_VALUE, codedInput.readTag());

            // invalid type
            try {
                codedInput.readCellPrimaryKeyValue();
                fail();
            } catch (IOException e) {
                assertEquals(
                        "Unsupported pk type: " + invalidType,
                        e.getMessage());
            } catch (Exception e) {
                fail("Unexpected Exception: " + e.getMessage());
            }
        }
    }

    @Test
    public void testSkipRawSize() throws IOException {
        int N = 100;
        // normal test
        {
            for (int i=0; i<N; i++) {
                byte[] data = new byte[N];
                data[i] = (byte)1;
                PlainBufferCodedInputStream codedInput = new PlainBufferCodedInputStream(new PlainBufferInputStream(data));

                codedInput.skipRawSize(i);
                assertEquals(1, codedInput.readTag());
            }
        }

        // fail test
        {
            byte[] data = new byte[N];
            PlainBufferCodedInputStream codedInput = new PlainBufferCodedInputStream(new PlainBufferInputStream(data));
            try {
                codedInput.skipRawSize(N+1);
                fail();
            } catch (IOException e) {
                assertEquals(PlainBufferConsts.READ_BYTE_EOF, e.getMessage());
            } catch (Exception e) {
                fail("Unexpected Exception: " + e.getMessage());
            }
        }
    }
}
