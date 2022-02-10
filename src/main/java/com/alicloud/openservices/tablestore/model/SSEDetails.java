package com.alicloud.openservices.tablestore.model;


import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class SSEDetails implements Jsonizable {

    /**
     * 是否开启服务器端加密
     */
    private boolean enable;

    /**
     * 秘钥类型
     * 开启服务器端加密时有效
     */
    private SSEKeyType keyType;

    /**
     * 主密钥在KMS中的id
     * 可以根据keyId在KMS系统中对秘钥的使用情况进行审计
     * 开启服务器端加密时有效
     */
    private String keyId;

    /**
     * 授权表格存储临时访问KMS用户主密钥的全局资源描述符
     * 开启服务器端加密且秘钥类型为SSE_BYOK时有效
     */
    private String roleArn;

    public SSEDetails() {

    }

    public SSEDetails(boolean enable, SSEKeyType keyType, String keyId, String roleArn) {
        setEnable(enable);
        setKeyType(keyType);
        setKeyId(keyId);
        setRoleArn(roleArn);
    }

    public SSEDetails(boolean enable, SSEKeyType keyType, byte[] keyId, byte[] roleArn) {
        setEnable(enable);
        setKeyType(keyType);
        setKeyId(keyId);
        setRoleArn(roleArn);
    }

    /**
     * 获取是否开启服务器端加密
     * @return 是否开启服务器端加密
     */
    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    /**
     * 获取服务器端加密的秘钥类型
     * @return 秘钥类型
     */
    public SSEKeyType getKeyType() {
        return keyType;
    }

    public void setKeyType(SSEKeyType keyType) {
        this.keyType = keyType;
    }

    /**
     * 获取主密钥在KMS中的id
     * @return 主密钥id
     */
    public String getKeyId() {
        return keyId;
    }

    public void setKeyId(String keyId) {
        this.keyId = keyId;
    }

    public void setKeyId(byte[] keyId) {
        if (keyId == null) {
            this.keyId = null;
        }
        this.keyId = new String(keyId);
    }

    /**
     * 获取授权表格存储临时访问KMS用户主密钥的全局资源描述符
     * @return 全局资源描述符
     */
    public String getRoleArn() {
        return roleArn;
    }

    public void setRoleArn(String roleArn) {
        this.roleArn = roleArn;
    }

    public void setRoleArn(byte[] roleArn) {
        if (roleArn == null) {
            this.roleArn = null;
        }
        this.roleArn = new String(roleArn);
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        sb.append(newline);
        sb.append("\"Enable\": ");
        sb.append(enable);
        sb.append(",");
        sb.append(newline);
        sb.append("\"keyType\": ");
        sb.append("\"" + keyType.toString() + "\"");
        sb.append(",");
        sb.append(newline);
        sb.append("\"keyId\": ");
        sb.append("\"" + keyId + "\"");
        sb.append(",");
        sb.append(newline);
        sb.append("\"roleArn\": ");
        sb.append("\"" + roleArn + "\"");
        sb.append(newline);
        sb.append("}");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Enable: ");
        sb.append(enable);
        sb.append(", keyType: ");
        sb.append(keyType.toString());
        sb.append(", keyId: ");
        sb.append(keyId);
        sb.append(", roleArn: ");
        sb.append(roleArn);
        return sb.toString();
    }
}
