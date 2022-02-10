package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class SSESpecification implements Jsonizable {

    /**
     * 是否开启服务器端加密
     */
    private boolean enable = false;

    /**
     * 当开启服务器端加密时，该参数用于设置秘钥类型
     */
    private OptionalValue<SSEKeyType> keyType = new OptionalValue<SSEKeyType>("KeyType");

    /**
     * 当开启服务器端加密且秘钥类型为BYOK时，该参数用于指定KMS用户主密钥的id
     */
    private OptionalValue<String> keyId = new OptionalValue<String>("KeyId");
 
    /**
     * 当开启服务器端加密且秘钥类型为BYOK时，
     * 需要通过STS服务授权表格存储获取临时访问令牌访问传入的KMS用户主密钥，
     * 该参数用于指定为此创建的RAM角色的全局资源描述符
     */
    private OptionalValue<String> roleArn = new OptionalValue<String>("RoleArn");

    /**
     * 构造一个SSESpecification对象。
     * 注意：传入的enable必须为false，原因是当enableStream为true时，必须指定keyType。
     *      如果需要设置开启服务器端加密，请使用另外两个构造函数。
     * @param enable 必须为false，代表关闭服务器端加密
     */
    public SSESpecification(boolean enable) {
        if (enable) {
            throw new ClientException("Key type is required when enable is true.");
        }
        setEnable(enable);
    }

    /**
     * 构造一个SSESpecification对象。
     * 注意：
     *   1. 传入的enable必须为true，原因是当enable为false时，不能指定keyType。
     *      如果需要设置关闭服务器端加密，请使用第一个构造函数。
     *   2. 传入的keyType必须为SSE_KMS_SERVICE，原因是当keyType为SSE_BYOK时，必须指定keyId和roleArn。
     *      如果需要设置加密类型为KMS服务主秘钥，请使用第三个构造函数
     * @param enable 必须为true，代表开启服务器端加密
     * @param keyType 必须为SSE_KMS_SERVICE，代表使用KMS的服务主密钥
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
     * 构造一个SSESpecification对象。
     * 注意：
     *   1. 传入的enable必须为true，原因是当enable为false时，不能指定keyType。
     *      如果需要设置关闭服务器端加密，请使用第一个构造函数。
     *   2. 传入的keyType必须为SSE_BYOK，原因是当keyType为SSE_KMS_SERVICE时，不能指定keyId和roleArn。
     *      如果需要设置加密类型为KMS用户主密钥，请使用第二个构造函数
     * @param enable 必须为true，代表开启服务器端加密
     * @param keyType 必须为SSE_BYOK，代表使用KMS的用户主密钥，支持用户自定义秘钥上传
     * @param keyId KMS用户主密钥的id
     * @param roleArn 授权表格存储临时访问keyId的全局资源描述符 
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
     * 返回是否开启服务器端加密
     *
     * @return enable
     */
    public boolean isEnable() {
        return enable;
    }

    /**
     * 设置是否开启服务器端加密
     *
     * @param enable
     */
    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    /**
     * 获取keyType参数
     *
     * @return keyType
     */
    public SSEKeyType getKeyType() {
        return keyType.getValue();
    }

    /**
     * 设置keyType参数
     *
     * @param keyType
     */
    public void setKeyType(SSEKeyType keyType) {
        this.keyType.setValue(keyType);
    }

    /**
     * 获取keyId参数
     *
     * @return keyId
     */
    public String getKeyId() {
        return keyId.getValue();
    }

    /**
     * 设置keyId参数
     *
     * @param keyId
     */
    public void setKeyId(String keyId) {
        this.keyId.setValue(keyId);
    }

    /**
     * 获取roleArn参数
     *
     * @return roleArn
     */
    public String getRoleArn() {
        return roleArn.getValue();
    }

    /**
     * 设置roleArn参数
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
