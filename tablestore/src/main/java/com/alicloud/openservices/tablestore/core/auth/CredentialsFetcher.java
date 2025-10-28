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

import java.io.IOException;
import java.net.URL;
import java.util.Map;

public interface CredentialsFetcher {

    /**
     * Construct the authorization server URL
     * 
     * @return the URL of the authorization server
     * @throws ClientException
     */
    public URL buildUrl() throws ClientException;

    /**
     * Get extra headers for the HTTP request.
     * @return
     */
    public Map<String, String> getExtraHeaders();

    /**
     * Sends an HTTP request to the authorization server
     *
     * @param request
     *            HTTP request
     * @return HTTP response
     * @throws IOException
     */
    public HttpResponse send(HttpRequest request) throws IOException;

    /**
     * Parse the authorization information returned by the authorization server and convert it into Credentials
     * 
     * @param response
     *            Authorization information returned by the authorization server
     * @return
     * @throws ClientException
     */
    public ServiceCredentials parse(HttpResponse response) throws ClientException;

    /**
     * Obtain authorization from the authorization server
     * 
     * @return credentials
     * @throws ClientException
     */
    public ServiceCredentials fetch() throws ClientException;

    /**
     * Obtain authorization from the authorization server
     * 
     * @param retryTimes
     *            retry times on failure
     * @return credentials
     * @throws ClientException
     */
    public ServiceCredentials fetch(int retryTimes) throws ClientException;
}
