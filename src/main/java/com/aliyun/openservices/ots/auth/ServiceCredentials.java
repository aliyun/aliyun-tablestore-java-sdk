/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.auth;

import static com.aliyun.openservices.ots.utils.CodingUtils.*;

/**
 * 表示用户访问的授权信息。
 *
 */
public class ServiceCredentials {
    private String accessKeyId;
    private String accessKeySecret;
    private String stsToken;

    /**
     * 获取访问用户的Access Key ID。
     * @return Access Key ID。
     */
    public String getAccessKeyId() {
        return accessKeyId;
    }

    /**
     * 设置访问用户的Access ID。
     * @param accessKeyId
     *          Access Key ID。
     */
    public void setAccessKeyId(String accessKeyId) {
        assertParameterNotNull(accessKeyId, "accessKeyId");
        this.accessKeyId = accessKeyId;
    }

    /**
     * 获取访问用户的Access Key Secret。
     * @return Access Key Secret。
     */
    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    /**
     * 设置访问用户的Access Key Secret。
     * @param accessKeySecret
     *          Access Key Secret。
     */
    public void setAccessKeySecret(String accessKeySecret) {
        assertParameterNotNull(accessKeySecret, "accessKeySecret");

        this.accessKeySecret = accessKeySecret;
    }

    /**
     * 构造函数。
     */
    public ServiceCredentials(){
    }

    /**
     * 构造函数。
     * @param accessKeyId
     *          Access Key ID。
     * @param accessKeySecret
     *          Access Key Secret。
     * @exception NullPointerException accessKeyId或accessKeySecret为空指针。
     */
    public ServiceCredentials(String accessKeyId, String accessKeySecret){
        setAccessKeyId(accessKeyId);
        setAccessKeySecret(accessKeySecret);
    }

    /**
     * 构造函数。
     * @param accessKeyId
     *          Access Key ID。
     * @param accessKeySecret
     *          Access Key Secret。
     * @exception NullPointerException accessKeyId或accessKeySecret为空指针。
     */
    public ServiceCredentials(String accessKeyId, String accessKeySecret, String stsToken){
        setAccessKeyId(accessKeyId);
        setAccessKeySecret(accessKeySecret);
        setStsToken(stsToken);
    }

    /**
     * 获取短期访问凭证。
     * 详细信息请参考阿里云STS (Security Token Service) 服务文档。
     * @return
     */
    public String getStsToken() {
        return stsToken;
    }

    /**
     * 设置短期访问凭证。
     * 详细信息请参考阿里云STS (Security Token Service) 服务文档。
     * @param stsToken
     */
    public void setStsToken(String stsToken) {
        this.stsToken = stsToken;
    }
}
