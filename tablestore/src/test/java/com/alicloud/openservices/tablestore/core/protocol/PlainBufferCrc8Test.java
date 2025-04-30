package com.alicloud.openservices.tablestore.core.protocol;

import org.junit.Test;
import java.util.Random;
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
    @Test
    public void testCrcFastPerf() {
        Random rand = new Random();
        long totalTimeCrc8InMill = 0;
        long totalTimeCrc8FastInMill = 0;
        for(int round = 0; round < 500; round++) {
            CalculateCrc8Result crc8Result;
            CalculateCrc8Result crc8FastResult;
            byte[] data = new byte[4 * 1024 * 1024];
            rand.nextBytes(data);
            if ((round % 2) == 0) {
                crc8Result = calculateCrc8CostTime(data);
                crc8FastResult = calculateCrc8FastCostTime(data);
            }else {
                crc8FastResult = calculateCrc8FastCostTime(data);
                crc8Result = calculateCrc8CostTime(data);
            }
            totalTimeCrc8FastInMill += crc8FastResult.costTime;
            totalTimeCrc8InMill += crc8Result.costTime;
            assertEquals(crc8FastResult.crc8, crc8Result.crc8);
        }
        System.out.println("totalTimeCrc8InMill:" + totalTimeCrc8InMill );
        System.out.println("totalTimeCrc8FastInMill:" + totalTimeCrc8FastInMill);
    }
    class CalculateCrc8Result {
        public long costTime;
        public byte crc8;
        public CalculateCrc8Result(long costTime, byte crc8) {
            this.costTime = costTime;
            this.crc8 = crc8;
        }
    }
    private CalculateCrc8Result calculateCrc8CostTime(byte[] data) {
        long start = System.currentTimeMillis();
        byte res = PlainBufferCrc8.crc8Slow((byte)0, data);
        long end = System.currentTimeMillis();
        return new CalculateCrc8Result(end-start, res);
    }
    private CalculateCrc8Result calculateCrc8FastCostTime(byte[] data) {
        long start = System.currentTimeMillis();
        byte res = PlainBufferCrc8.crc8Fast((byte)0, data);
        long end = System.currentTimeMillis();
        return new CalculateCrc8Result(end-start, res);
    }
}
