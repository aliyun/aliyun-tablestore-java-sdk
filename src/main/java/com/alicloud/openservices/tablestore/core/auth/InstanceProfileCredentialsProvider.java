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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Credentials provider implementation that loads credentials from the Ali Cloud
 * ECS Instance Metadata Service.
 */
public class InstanceProfileCredentialsProvider implements CredentialsProvider {

    private static Logger logger = LoggerFactory.getLogger(InstanceProfileCredentialsProvider.class);

    public InstanceProfileCredentialsProvider(String roleName) {
        if (null == roleName) {
            throw new NullPointerException("You must specify a valid role name.");
        }
        this.roleName = roleName;
        this.fetcher = new InstanceProfileCredentialsFetcher();
        this.fetcher.setRoleName(this.roleName);
    }
    
    public InstanceProfileCredentialsProvider withCredentialsFetcher(InstanceProfileCredentialsFetcher fetcher) {
        this.fetcher = fetcher;
        return this;
    }

    @Override
    public void setCredentials(ServiceCredentials creds) {

    }

    public boolean isCredentialsInvalid() {
        return credentials == null || credentials.isExpired();
    }

    public boolean shouldRefreshCredentials() {
        return !isFetching.get() && credentials.willSoonExpire() && credentials.shouldRefresh();
    }

    @Override
    public InstanceProfileCredentials getCredentials() {
        if (isCredentialsInvalid() || shouldRefreshCredentials()) {
            synchronized (this) {
                if (isCredentialsInvalid()) {
                    try {
                        credentials = (InstanceProfileCredentials) fetcher.fetch(maxRetryTimes);
                    } catch (ClientException e) {
                        logger.error("EcsInstanceCredentialsFetcher.fetch Exception:", e);
                        return null;
                    }
                } else if (shouldRefreshCredentials()) {
                    try {
                        isFetching.set(true);
                        credentials = (InstanceProfileCredentials) fetcher.fetch();
                    } catch (ClientException e) {
                        // Use the current expiring session token and wait for next round
                        credentials.setLastFailedRefreshTime();
                        logger.error("EcsInstanceCredentialsFetcher.fetch Exception:", e);
                    } finally {
                        isFetching.set(false);
                    }
                }
            }
        }

        return credentials;
    }

    private final String roleName;
    private volatile InstanceProfileCredentials credentials;
    private InstanceProfileCredentialsFetcher fetcher;

    private final AtomicBoolean isFetching = new AtomicBoolean(false);
    private int maxRetryTimes = AuthUtils.MAX_ECS_METADATA_FETCH_RETRY_TIMES;

}
