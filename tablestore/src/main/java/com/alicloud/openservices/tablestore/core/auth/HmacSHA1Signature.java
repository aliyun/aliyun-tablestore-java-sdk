package com.alicloud.openservices.tablestore.core.auth;

import com.alicloud.openservices.tablestore.core.utils.Base64;
import com.alicloud.openservices.tablestore.core.utils.Bytes;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class HmacSHA1Signature implements ServiceSignature {
    private static final String ALGORITHM = "HmacSHA1"; // Signature method.
    private static final Object LOCK = new Object();
    private static Mac macInstance; // Prototype of the Mac instance.

    private Mac mac;

    public String getAlgorithm() {
        return ALGORITHM;
    }

    public HmacSHA1Signature(byte[] key) {
        try {
            // Because Mac.getInstance(String) calls a synchronized method,
            // it could block on invoked concurrently.
            // SO use prototype pattern to improve performance.
            if (macInstance == null) {
                synchronized (LOCK) {
                    if (macInstance == null) {
                        macInstance = Mac.getInstance(ALGORITHM);
                    }
                }
            }

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
