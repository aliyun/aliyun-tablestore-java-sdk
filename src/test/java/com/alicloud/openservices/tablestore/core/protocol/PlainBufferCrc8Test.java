package com.alicloud.openservices.tablestore.core.protocol;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import static com.alicloud.openservices.tablestore.core.protocol.PlainBufferCrc8.*;

public class PlainBufferCrc8Test {

    @Test
    public void testByte() {
        byte c8 = 0;
        c8 = crc8(c8, (byte)1);
        assertEquals(7, c8);
        c8 = crc8(c8, (byte)3);
        assertEquals(28, c8);
        c8 = crc8(c8, (byte)7);
        assertEquals(65, c8);
        c8 = crc8(c8, (byte)255);
        assertEquals(51, c8);
        c8 = crc8(c8, (byte)128);
        assertEquals(16, c8);
        c8 = crc8(c8, (byte)0);
        assertEquals(112, c8);
    }
}
