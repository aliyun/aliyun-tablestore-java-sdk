package com.aliyun.openservices.ots.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class OTSCompressUtil {
    
    static final int MIN_BUFFER_SIZE = 10240; // 10KB

    public static byte[] compress(InputStream input, Deflater compresser) throws IOException{
        int avaiableLength = input.available();
        int estimateLength = avaiableLength > 0 ? avaiableLength : 1024;
        ByteArrayOutputStream output = new ByteArrayOutputStream(estimateLength);
        try{
            byte[] in = new byte[estimateLength];
            byte[] outBuffer = new byte[estimateLength];
            do{
                int len = input.read(in);
                /**
                 * If input stream has reach the end, user should explicitly call finish(), only that compresser can be finished.
                 */
                if(len == -1) {
                    compresser.finish(); // no more input, compression should end with the current contents of the input
                    while(!compresser.finished()) {
                        int bytesCompressed = compresser.deflate(outBuffer);
                        if(bytesCompressed > 0){
                            output.write(outBuffer, 0, bytesCompressed);
                        }
                    }
                    break; //finished
                }
                else {
                    compresser.setInput(in, 0, len);
                    while(!compresser.needsInput()) { // needsInput = true doesn't mean that all compressed data is flush to output buffer 
                        int bytesCompressed = compresser.deflate(outBuffer);
                        if(bytesCompressed > 0){
                            output.write(outBuffer, 0, bytesCompressed);
                        }
                    }
                }
            } while(true);
            output.flush();
            return output.toByteArray();
        }
        finally{
            output.close();
        }
    }

    public static byte[] decompress(InputStream input, int rawDataSize, Inflater decompresser) throws DataFormatException, IOException{
        ByteArrayOutputStream output = new ByteArrayOutputStream(rawDataSize);
        int av = input.available();
        int bufferSize = av < MIN_BUFFER_SIZE ? MIN_BUFFER_SIZE : av;
        try{
            byte[] in = new byte[bufferSize];
            byte[] outBuffer = new byte[rawDataSize];
            do{
                int len = input.read(in);
                if(len == -1) 
                {
                    // no more input
                    while(!decompresser.finished()) {
                        int bytesDecompressed = decompresser.inflate(outBuffer);
                        if(bytesDecompressed > 0){
                            output.write(outBuffer, 0, bytesDecompressed);
                        }
                    }
                    break;
                }
                else
                {
                    decompresser.setInput(in, 0, len);
                    while(!decompresser.needsInput()) {
                        int bytesDecompressed = decompresser.inflate(outBuffer);
                        if(bytesDecompressed > 0){
                            output.write(outBuffer, 0, bytesDecompressed);
                        }
                    }
                }
            } while(true);
            output.flush();
            return output.toByteArray();
        }
        finally{
            output.close();
        }
    }
}
