package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class SSESpecification implements Jsonizable {

    /**
     * Whether to enable server-side encryption
     */
    private boolean enable = false;

    /**
     * When server-side encryption is enabled, this parameter is used to set the key type.
     */
    private OptionalValue<SSEKeyType> keyType = new OptionalValue<SSEKeyType>("KeyType");

    /**
     * When server-side encryption is enabled and the key type is BYOK, this parameter is used to specify the id of the KMS user master key.
     */
    private OptionalValue<String> keyId = new OptionalValue<String>("KeyId");
 
    /**
     * When server-side encryption is enabled and the key type is BYOK,
     * you need to authorize Table Store to obtain a temporary access token for the specified KMS customer master key via STS service.
     * This parameter is used to specify the global resource descriptor of the RAM role created for this purpose.
     */
    private OptionalValue<String> roleArn = new OptionalValue<String>("RoleArn");

    /**
     * Construct an SSESpecification object.
     * Note: The input enable must be false, because when enableStream is true, keyType must be specified.
     *       If you need to enable server-side encryption, please use the other two constructors.
     * @param enable Must be false, indicating that server-side encryption is disabled
     */
    public SSESpecification(boolean enable) {
        if (enable) {
            throw new ClientException("Key type is required when enable is true.");
        }
        setEnable(enable);
    }

    /**
     * Construct an SSESpecification object.
     * Note:
     *   1. The input 'enable' must be true, because when 'enable' is false, the 'keyType' cannot be specified.
     *      If you need to set server-side encryption to off, please use the first constructor.
     *   2. The input 'keyType' must be SSE_KMS_SERVICE, because when 'keyType' is SSE_BYOK, 'keyId' and 'roleArn' must be specified.
     *      If you need to set the encryption type to KMS managed service keys, please use the third constructor.
     * @param enable Must be true, indicating that server-side encryption is enabled
     * @param keyType Must be SSE_KMS_SERVICE, indicating the use of KMS managed service keys
     */
    public SSESpecification(boolean enable, SSEKeyType keyType) {
        if (!enable) {
            throw new ClientException("Key type cannot be set when enable is false.");
        }
        if (keyType != SSEKeyType.SSE_KMS_SERVICE) {
            throw new ClientException("Key id and role arn are required when key type is not SSE_KMS_SERVICE.");
        }
        setEnable(enable);
        setKeyType(keyType);
    }

    /**
     * Constructs an SSESpecification object.
     * Note:
     *   1. The input enable must be true, because when enable is false, keyType cannot be specified.
     *      If you need to set server-side encryption to off, please use the first constructor.
     *   2. The input keyType must be SSE_BYOK, because when keyType is SSE_KMS_SERVICE, keyId and roleArn cannot be specified.
     *      If you need to set the encryption type to KMS user master key, please use the second constructor.
     * @param enable Must be true, indicating that server-side encryption is enabled.
     * @param keyType Must be SSE_BYOK, indicating the use of a KMS user master key, supporting user-defined key upload.
     * @param keyId The id of the KMS user master key.
     * @param roleArn The global resource descriptor authorizing Table Store to temporarily access keyId.
     */
    public SSESpecification(boolean enable, SSEKeyType keyType, String keyId, String roleArn) {
        if (!enable) {
            throw new ClientException("Key type cannot be set when enable is false.");
        }
        if (keyType != SSEKeyType.SSE_BYOK) {
            throw new ClientException("Key id and role arn cannot be set when key type is not SSE_BYOK.");
        }
        setEnable(enable);
        setKeyType(keyType);
        setKeyId(keyId);
        setRoleArn(roleArn);
    }

    /**
     * Returns whether server-side encryption is enabled
     *
     * @return enable
     */
    public boolean isEnable() {
        return enable;
    }

    /**
     * Set whether to enable server-side encryption
     *
     * @param enable
     */
    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    /**
     * Get the keyType parameter
     *
     * @return keyType
     */
    public SSEKeyType getKeyType() {
        return keyType.getValue();
    }

    /**
     * Set the keyType parameter
     *
     * @param keyType
     */
    public void setKeyType(SSEKeyType keyType) {
        this.keyType.setValue(keyType);
    }

    /**
     * Get the keyId parameter
     *
     * @return keyId
     */
    public String getKeyId() {
        return keyId.getValue();
    }

    /**
     * Set the keyId parameter
     *
     * @param keyId
     */
    public void setKeyId(String keyId) {
        this.keyId.setValue(keyId);
    }

    /**
     * Get the roleArn parameter
     *
     * @return roleArn
     */
    public String getRoleArn() {
        return roleArn.getValue();
    }

    /**
     * Set the roleArn parameter
     *
     * @param roleArn
     */
    public void setRoleArn(String roleArn) {
        this.roleArn.setValue(roleArn);
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
        if (keyType.isValueSet()) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"KeyType\": ");
            sb.append(keyType.getValue().toString());
            sb.append(newline);
        }
        if (keyId.isValueSet()) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"KeyId\": ");
            sb.append(keyId.getValue());
            sb.append(newline);
        }
        if (roleArn.isValueSet()) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"RoleArn\": ");
            sb.append(roleArn.getValue());
            sb.append(newline);
        }
        sb.append("}");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Enable: ");
        sb.append(enable);
        if (keyType.isValueSet()) {
            sb.append(", KeyType: ");
            sb.append(keyType.getValue().toString());
        }
        if (keyId.isValueSet()) {
            sb.append(", KeyId: ");
            sb.append(keyId.getValue());
        }
        if (roleArn.isValueSet()) {
            sb.append(", RoleArn: ");
            sb.append(roleArn.getValue());
        }
        return sb.toString();
    }
}
