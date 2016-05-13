package com.aliyun.openservices.ots.auth;

/**
 * 
 * 表示用于计算访问签名的接口。
 *
 */
public abstract class ServiceSignature {
    public ServiceSignature(){
    }

    /**
     * 获取签名的算法。
     * @return 签名算法。
     */
    public abstract String getAlgorithm();
    
    /**
     * 获取签名算法的版本信息。
     * @return 签名算法的版本。
     */
    public abstract String getVersion();
    
    /**
     * 计算签名。
     * @param key 签名所需的密钥，对应于访问的Access Key。
     * @param data 用于计算签名的字符串信息。
     * @return 签名字符串。
     */
    public abstract String computeSignature(String key, String data);

    /**
     * 创建默认的<code>ServiceSignature</code>实例。
     * @return 默认的<code>ServiceSignature</code>实现。
     */
    public static ServiceSignature create(){
        return new HmacSHA1Signature();
    }
}