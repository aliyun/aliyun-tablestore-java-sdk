package com.alicloud.openservices.tablestore.model;


import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class SSEDetails implements Jsonizable {

    /**
     * Whether to enable server-side encryption
     */
    private boolean enable;

    /**
     * Key type
     * Valid when server-side encryption is enabled
     */
    private SSEKeyType keyType;

    /**
     * The ID of the master key in KMS
     * You can audit the usage of the key in the KMS system according to the keyId
     * Valid when server-side encryption is enabled
     */
    private String keyId;

    /**
     * Authorizes the global resource descriptor of the KMS user master key for temporary access to Table Store.
     * This is valid when server-side encryption is enabled and the key type is SSE_BYOK.
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
     * Get whether server-side encryption is enabled
     * @return Whether server-side encryption is enabled
     */
    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    /**
     * Get the type of server-side encryption key
     * @return Key type
     */
    public SSEKeyType getKeyType() {
        return keyType;
    }

    public void setKeyType(SSEKeyType keyType) {
        this.keyType = keyType;
    }

    /**
     * Get the ID of the main key in KMS
     * @return Main key ID
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
     * Get the global resource descriptor for the temporary access KMS user master key authorization of Tablestore.
     * @return Global resource descriptor
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
