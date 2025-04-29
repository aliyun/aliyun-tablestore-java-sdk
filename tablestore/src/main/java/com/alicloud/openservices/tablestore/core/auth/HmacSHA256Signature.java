package com.alicloud.openservices.tablestore.core.auth;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.alicloud.openservices.tablestore.core.utils.Base64;
import com.alicloud.openservices.tablestore.core.utils.Bytes;

public class HmacSHA256Signature implements ServiceSignature {
    private static final String ALGORITHM = "HmacSHA256"; // Signature method.
    private static final Object LOCK = new Object();
    private static Mac macInstance; // Prototype of the Mac instance.

    private Mac mac;

    static {
        synchronized (LOCK) {
            if (macInstance == null) {
                try {
                    macInstance = Mac.getInstance(ALGORITHM);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public HmacSHA256Signature(byte[] key) {
        try {
            try {
                mac = (Mac) macInstance.clone();
            } catch (CloneNotSupportedException e) {
                // If it is not cloneable, create a new DefaultSigner
                mac = Mac.getInstance(ALGORITHM);
            }
            mac.init(new SecretKeySpec(key, ALGORITHM));
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("Unsupported algorithm: " + ALGORITHM, ex);
        } catch (InvalidKeyException ex) {
            throw new RuntimeException(ex);
        }
    }

    public String getAlgorithm() {
        return ALGORITHM;
    }

    public void updateUTF8String(String data) {
        update(Bytes.toBytes(data));
    }

    public void update(byte[] data) {
        mac.update(data);
    }

    @Override
    public void update(byte data) {
        mac.update(data);
    }

    public String computeSignature() {
        byte[] signature = mac.doFinal();
        return Base64.toBase64String(signature);
    }
}
