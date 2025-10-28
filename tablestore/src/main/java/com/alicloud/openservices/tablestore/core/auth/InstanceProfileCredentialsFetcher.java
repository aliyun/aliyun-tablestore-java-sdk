/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.alicloud.openservices.tablestore.core.auth;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.utils.HttpRequest;
import com.alicloud.openservices.tablestore.core.utils.HttpResponse;
import com.alicloud.openservices.tablestore.core.utils.MethodType;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstanceProfileCredentialsFetcher extends HttpCredentialsFetcher {

    private static Logger logger = LoggerFactory.getLogger(InstanceProfileCredentialsFetcher.class);

    public InstanceProfileCredentialsFetcher() {
    }

    public void setRoleName(String roleName) {
        if (null == roleName || roleName.isEmpty()) {
            throw new IllegalArgumentException("You must specifiy a valid role name.");
        }
        this.roleName = roleName;
    }

    public InstanceProfileCredentialsFetcher withRoleName(String roleName) {
        setRoleName(roleName);
        return this;
    }

    @Override
    public URL buildUrl() throws ClientException {
        try {
            return new URL("http://" + metadataServiceHost + URL_IN_ECS_METADATA + roleName);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e.toString());
        }
    }

    public URL buildFetchTokenUrl() throws ClientException {
        try {
            return new URL("http://" + metadataServiceHost + URL_IN_ECS_TOKEN);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e.toString());
        }
    }

    public Map<String, String> getExtraHeaders() {
        return Collections.singletonMap(HEADER_ECS_METADATA_TOKEN, getToken());
    }

    private String getToken() {
        if (null == token || isTokenExpired()) {
             synchronized (this) {
                 if (null == token || isTokenExpired()) {
                     refreshToken();
                 }
             }
        }

        return token;
    }

    private boolean isTokenExpired() {
        return System.currentTimeMillis() - tokenTimestamp > (DEFAULT_TOKEN_TTL_IN_SECONDS - TOKEN_EXPIRATION_BUFFER_IN_SECONDS) * 1000;
    }

    private void refreshToken() {
        URL url = buildFetchTokenUrl();
        HttpRequest request = new HttpRequest(url.toString());
        request.setMethod(MethodType.PUT);
        request.putHeaderParameter(HEADER_ECS_METADATA_TOKEN_TTL, String.valueOf(DEFAULT_TOKEN_TTL_IN_SECONDS));
        request.setConnectTimeout(AuthUtils.DEFAULT_HTTP_SOCKET_TIMEOUT_IN_MILLISECONDS);
        request.setReadTimeout(AuthUtils.DEFAULT_HTTP_SOCKET_TIMEOUT_IN_MILLISECONDS);

        HttpResponse response = null;
        try {
            response = send(request);
        } catch (IOException e) {
            logger.error("InstanceProfileCredentialsFetcher.refreshToken failed.", e);
            throw new ClientException("InstanceProfileCredentialsFetcher.refreshToken exception: " + e);
        }

        if (response.getHttpContent() == null) {
            logger.error("Invalid response from ECS Metadata service: null.");
            throw new ClientException("Invalid response got from ECS Metadata service.");
        }

        token = new String(response.getHttpContent());
        tokenTimestamp = System.currentTimeMillis();
        logger.info("Refreshed RAM session credentials token: {}.", token);
    }

    private static class MetadataResponse {
        @SerializedName("Code")
        String code;

        @SerializedName("AccessKeyId")
        String accessKeyId;

        @SerializedName("AccessKeySecret")
        String accessKeySecret;

        @SerializedName("SecurityToken")
        String securityToken;

        @SerializedName("Expiration")
        String expiration;
    }

    @Override
    public ServiceCredentials parse(HttpResponse response) throws ClientException {
        if (response.getHttpContent() == null) {
            logger.error("Invalid response from ECS Metadata service: null.");
            throw new ClientException("Invalid json got from ECS Metadata service.");
        }

        String jsonContent = new String(response.getHttpContent());

        try {
            MetadataResponse res = new Gson().fromJson(jsonContent, MetadataResponse.class);
            if (res == null) {
                logger.error("Invalid response from ECS Metadata service: {}.", jsonContent);
                throw new ClientException("Invalid json got from ECS Metadata service.");
            }

            if (!(res.code != null && res.accessKeyId != null && res.accessKeySecret != null
                && res.securityToken != null && res.expiration != null)) {
                logger.error("Invalid response from ECS Metadata service: {}.", jsonContent);
                throw new ClientException("Invalid json got from ECS Metadata service.");
            }

            if (!"Success".equalsIgnoreCase(res.code)) {
                logger.error("Failed to get RAM session credentials from ECS Metadata service: {}.", jsonContent);
                throw new ClientException("Failed to get RAM session credentials from ECS metadata service.");
            }

            return new InstanceProfileCredentials(res.accessKeyId, res.accessKeySecret, res.securityToken, res.expiration);
        } catch (Exception e) {
            throw new ClientException("InstanceProfileCredentialsFetcher.parse [" + jsonContent + "] exception:", e);
        }
    }


    private static final String HEADER_ECS_METADATA_TOKEN_TTL = "X-aliyun-ecs-metadata-token-ttl-seconds";
    private static final String HEADER_ECS_METADATA_TOKEN = "X-aliyun-ecs-metadata-token";

    private static final String URL_IN_ECS_TOKEN = "/latest/api/token";
    private static final String URL_IN_ECS_METADATA = "/latest/meta-data/ram/security-credentials/";
    private static final String metadataServiceHost = "100.100.100.200";

    private static final int DEFAULT_TOKEN_TTL_IN_SECONDS = 21600;
    private static final int TOKEN_EXPIRATION_BUFFER_IN_SECONDS = 600;

    private String token;
    private long tokenTimestamp = 0;
    private String roleName;

}
