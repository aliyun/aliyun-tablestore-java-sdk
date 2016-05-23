package com.aliyun.openservices.ots;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Random;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.junit.Test;

import com.aliyun.openservices.ots.internal.OTSCompressUtil;

public class OTSCompressUtilTest {
    @Test
    public void testCompressAndDecompress() throws IOException, DataFormatException{
        final int dataLength = 10 * 1024 * 1024 + 137;
        byte[] bytes = new byte[dataLength];
        Random random = new Random();
        for(int i = 0; i < dataLength; i++){
            bytes[i] = (byte)random.nextInt(128);
        }
        Deflater compresser = new Deflater();
        Inflater decompresser = new Inflater();
        
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        byte[] compressedBytes = OTSCompressUtil.compress(input, compresser);
        ByteArrayInputStream compressedData = new ByteArrayInputStream(compressedBytes);
        byte[] decompressedBytes = OTSCompressUtil.decompress(compressedData, bytes.length, decompresser);
        assertEquals(dataLength, decompressedBytes.length);
        for(int i = 0; i < dataLength; i++){
            assertEquals(bytes[i], decompressedBytes[i]);
        }   
    }
}
