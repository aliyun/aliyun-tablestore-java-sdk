package com.alicloud.openservices.tablestore.core.auth;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.alicloud.openservices.tablestore.core.utils.Base64;

public class CalculateV4SigningKeyUtil {
    private static String PREFIX = "aliyun_v4";
    private static String CONSTANT = "aliyun_v4_request";
    private static final Object LOCK = new Object();
    private static Mac macInstance; // Prototype of the Mac instance.

    /**
     * 获取一级派生秘钥
     *
     * @param secret
     * @param date
     * @param signMethod HmacSHA256
     * @return
     */
    public static byte[] firstSigningKey(String secret, String date, String signMethod) {
        Mac mac = null;
        try {
            mac = initMac(signMethod);
            mac.init(new SecretKeySpec((PREFIX + secret).getBytes("UTF-8"), signMethod));
            return mac.doFinal(date.getBytes("UTF-8"));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("unsupport Algorithm:" + signMethod);
        } catch (InvalidKeyException e) {
            throw new RuntimeException("InvalidKey");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UnsupportedEncoding");
        }
    }

    /**
     * 获取 date+region派生秘钥
     *
     * @param secret
     * @param date
     * @param region
     * @param signMethod
     * @return
     */
    public static byte[] regionSigningKey(String secret, String date, String region, String signMethod) {
        byte[] firstSignkey = firstSigningKey(secret, date, signMethod);
        Mac mac = null;
        try {
            mac = initMac(signMethod);
            mac.init(new SecretKeySpec(firstSignkey, signMethod));
            return mac.doFinal(region.getBytes("UTF-8"));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("unsupport Algorithm:" + signMethod);
        } catch (InvalidKeyException e) {
            throw new RuntimeException("InvalidKey");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UnsupportedEncoding");
        }
    }

    /**
     * 返回base64编码的region级别派生秘钥
     *
     * @param secret
     * @param date
     * @param region
     * @param signMethod
     * @return
     */
    public static String regionSigningKeyString(String secret, String date, String region, String signMethod) {
        return Base64.toBase64String(regionSigningKey(secret, date, region, signMethod));
    }

    /**
     * @param secret
     * @param date
     * @param region
     * @param productCode
     * @param signMethod
     * @return
     */
    public static byte[] finalSigningKey(String secret, String date, String region, String productCode, String signMethod) {
        byte[] secondSignify = regionSigningKey(secret, date, region, signMethod);
        Mac mac = null;
        try {
            mac = initMac(signMethod);
            mac.init(new SecretKeySpec(secondSignify, signMethod));
            byte[] thirdSigningKey = mac.doFinal(productCode.getBytes("UTF-8"));
            // 计算最终派生秘钥
            mac = Mac.getInstance(signMethod);
            mac.init(new SecretKeySpec(thirdSigningKey, signMethod));
            return mac.doFinal(CONSTANT.getBytes("UTF-8"));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("unsupport Algorithm:" + signMethod);
        } catch (InvalidKeyException e) {
            throw new RuntimeException("InvalidKey");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UnsupportedEncoding");
        }
    }

    /**
     * 返回base64编码的最终派生秘钥
     *
     * @param secret
     * @param date
     * @param region
     * @param productCode
     * @param signMethod
     * @return
     */
    public static String finalSigningKeyString(String secret, String date, String region, String productCode, String signMethod) {
        return Base64.toBase64String(finalSigningKey(secret, date, region, productCode, signMethod));
    }


    private static Mac initMac(String signMethod) throws NoSuchAlgorithmException {
        if (macInstance == null) {
            synchronized (LOCK) {
                if (macInstance == null) {
                    macInstance = Mac.getInstance(signMethod);
                }
            }
        }
        try {
            return (Mac) macInstance.clone();
        } catch (CloneNotSupportedException e) {
            return Mac.getInstance(signMethod);
        }
    }
}
