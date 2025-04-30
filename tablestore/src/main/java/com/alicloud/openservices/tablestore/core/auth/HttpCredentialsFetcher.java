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

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.utils.HttpRequest;
import com.alicloud.openservices.tablestore.core.utils.HttpResponse;
import com.alicloud.openservices.tablestore.core.utils.MethodType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public abstract class HttpCredentialsFetcher implements CredentialsFetcher {
    private static Logger logger = LoggerFactory.getLogger(HttpCredentialsFetcher.class);
    
    public abstract URL buildUrl() throws ClientException;
    
    public abstract ServiceCredentials parse(HttpResponse response) throws ClientException;
    
    @Override
    public ServiceCredentials fetch() throws ClientException {
        URL url = buildUrl();
        HttpRequest request = new HttpRequest(url.toString());
        request.setMethod(MethodType.GET);
        request.setConnectTimeout(AuthUtils.DEFAULT_HTTP_SOCKET_TIMEOUT_IN_MILLISECONDS);
        request.setReadTimeout(AuthUtils.DEFAULT_HTTP_SOCKET_TIMEOUT_IN_MILLISECONDS);
        
        HttpResponse response = null;
        try {
            response = send(request);
        } catch (IOException e) {
            logger.error("CredentialsFetcher.fetch failed.", e);
            throw new ClientException("CredentialsFetcher.fetch exception: " + e);
        }
        
        return parse(response);
    }
    
    @Override
    public HttpResponse send(HttpRequest request) throws IOException {
        HttpResponse response = HttpResponse.getResponse(request);
        if (response.getStatus() != HttpURLConnection.HTTP_OK) {
            throw new IOException("HttpCode=" + response.getStatus());
        }
        return response;
    }
    
    @Override
    public ServiceCredentials fetch(int retryTimes) throws ClientException {
        for (int i = 1; i <= retryTimes; i++) {
            try {
                return fetch();
            } catch (Exception e) {
                if (i == retryTimes) {
                    throw new ClientException(e);
                }

                // simple retry strategy
                try {
                    Thread.sleep(retryTimes * 100);
                } catch (InterruptedException ie) {
                }
            }
        }

        logger.error("Failed to connect ECS Metadata Service after retry '{}' times.", retryTimes);
        throw new ClientException("Failed to connect ECS Metadata Service: Max retry times exceeded.");
    }
}
