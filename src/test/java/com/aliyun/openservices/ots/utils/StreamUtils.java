package com.aliyun.openservices.ots.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.security.DigestInputStream;
import java.security.MessageDigest;

public class StreamUtils {

    public static String readContent(InputStream contentStream, String charset)
            throws UnsupportedEncodingException{

        InputStreamReader reader =
                new InputStreamReader(contentStream, charset);
        BufferedReader br = new BufferedReader(reader);
        StringBuilder sb = new StringBuilder();
        String line = null;
        try {
            while((line = br.readLine()) != null){
                sb.append(line);
            }
            return sb.toString();
        } catch (IOException e) {
            return null;
        }
        finally{
            try {
                br.close();
                reader.close();
            } catch (IOException e) {
            }
        }
    }

    public static String calculateMD5(InputStream in) throws Exception {

        DigestInputStream ds = new DigestInputStream(in, MessageDigest.getInstance("MD5"));
        while (ds.read() != -1) {
        }

        byte[] md5bytes = ds.getMessageDigest().digest();

        StringBuilder sb = new StringBuilder();
        for (byte md5byte : md5bytes) {
            String hexBiChars = String.format("%02x", md5byte);
            sb.append(hexBiChars);
        }

        return sb.toString();
    }
}
