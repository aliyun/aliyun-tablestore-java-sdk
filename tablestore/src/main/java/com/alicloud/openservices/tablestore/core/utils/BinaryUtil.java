/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 *
 * Copyright (C) Alibaba Cloud Computing
 */

package com.alicloud.openservices.tablestore.core.utils;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class BinaryUtil {
    private static MessageDigest messageDigestMd5;
    private static final Object LOCK = new Object();

    public static byte[] calculateMd5(byte[] binaryData) {

        // Use the prototype pattern to improve performance under multi-threaded high concurrency.
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

    public static String toString(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < buffer.capacity(); i++) {
            sb.append(" ").append(buffer.get(i) & 0xff);
        }
        return sb.toString();
    }
}
