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

import com.alicloud.openservices.tablestore.core.utils.HttpUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * Default implementation of {@link CredentialsProvider}.
 */
public class DefaultCredentialProvider implements CredentialsProvider {

    private volatile ServiceCredentials creds;

    public DefaultCredentialProvider(ServiceCredentials creds) {
        setCredentials(creds);
    }

    public DefaultCredentialProvider(String accessKeyId, String secretAccessKey) {
        this(accessKeyId, secretAccessKey, null);
    }

    public DefaultCredentialProvider(String accessKeyId, String secretAccessKey, String securityToken) {
        checkCredentials(accessKeyId, secretAccessKey);
        setCredentials(new DefaultCredentials(accessKeyId, secretAccessKey, securityToken));
    }

    @Override
    public synchronized void setCredentials(ServiceCredentials creds) {
        if (creds == null) {
            throw new InvalidCredentialsException("creds should not be null.");
        }

        checkCredentials(creds.getAccessKeyId(), creds.getAccessKeySecret());
        this.creds = creds;
    }

    @Override
    public ServiceCredentials getCredentials() {
        if (this.creds == null) {
            throw new InvalidCredentialsException("Invalid credentials");
        }

        return this.creds;
    }

    private static void checkCredentials(String accessKeyId, String secretAccessKey) {
        if (accessKeyId == null || accessKeyId.equals("")) {
            throw new InvalidCredentialsException("Access key id should not be null or empty.");
        }

        if (secretAccessKey == null || secretAccessKey.equals("")) {
            throw new InvalidCredentialsException("Access key secret should not be null or empty.");
        }

        Preconditions.checkArgument(AuthUtils.checkAccessKeyIdFormat(accessKeyId), "The access key id is invalid: " + accessKeyId);
    }

}
