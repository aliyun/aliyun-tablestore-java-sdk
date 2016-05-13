package com.aliyun.openservices.ots.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

public class BinaryUtil {
    private static MessageDigest messageDigestMd5;
    private static final Object LOCK = new Object();

    public static String toBase64String(byte[] binaryData) {
        return DatatypeConverter.printBase64Binary(binaryData);
    }

    public static byte[] fromBase64String(String base64String) {
        return DatatypeConverter.parseBase64Binary(base64String);
    }


    public static byte[] calculateMd5(byte[] binaryData) {
        
        //采用prototype模式，提高多线程高并发下的性能
        if (messageDigestMd5 == null) {
            synchronized (LOCK) {
                if (messageDigestMd5 == null) {
                    try {
                        messageDigestMd5 = MessageDigest.getInstance("MD5");
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException("MD5 algorithm not found.");
                    }
                }
            }
        }
        MessageDigest messageDigest = null;
        try {
            messageDigest = (MessageDigest) messageDigestMd5.clone();
        } catch (CloneNotSupportedException e) {
            try {
                messageDigest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e1) {
                throw new RuntimeException("MD5 algorithm not found.");
            }
        }
        messageDigest.update(binaryData);
        return messageDigest.digest();
    }
}
