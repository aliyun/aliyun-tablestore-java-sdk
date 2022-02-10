package com.alicloud.openservices.tablestore.model;

/**
 * 表示服务器端加密的秘钥类型
 */
public enum SSEKeyType {
    /**
     * 使用KMS的服务主密钥
     */
	SSE_KMS_SERVICE,
    /**
     * 使用KMS的用户主密钥，支持用户自定义秘钥上传
     */
        SSE_BYOK;
}
