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

/**
 * Credentials provider factory to share providers across potentially many
 * clients.
 */
public class CredentialsProviderFactory {

    /**
     * Create an instance of DefaultCredentialProvider.
     * 
     * @param accessKeyId
     *            Access Key ID.
     * @param secretAccessKey
     *            Secret Access Key.
     * @return A {@link DefaultCredentialProvider} instance.
     */
    public static DefaultCredentialProvider newDefaultCredentialProvider(String accessKeyId, String secretAccessKey) {
        return new DefaultCredentialProvider(accessKeyId, secretAccessKey);
    }

    /**
     * Create an instance of DefaultCredentialProvider.
     * 
     * @param accessKeyId
     *            Access Key ID.
     * @param secretAccessKey
     *            Secret Access Key.
     * @param securityToken
     *            Security Token from STS.
     * @return A {@link DefaultCredentialProvider} instance.
     */
    public static DefaultCredentialProvider newDefaultCredentialProvider(String accessKeyId, String secretAccessKey,
                                                                  String securityToken) {
        return new DefaultCredentialProvider(accessKeyId, secretAccessKey, securityToken);
    }

    /**
     * Create an instance of EnvironmentVariableCredentialsProvider by reading
     * the environment variable to obtain the ak/sk, such as TABLESTORE_ACCESS_KEY_ID
     * and TABLESTORE_ACCESS_KEY_SECRET
     * 
     * @return A {@link EnvironmentVariableCredentialsProvider} instance.
     * @throws ClientException
     *             TableStore Client side exception.
     */
    public static EnvironmentVariableCredentialsProvider newEnvironmentVariableCredentialsProvider()
            throws ClientException {
        return new EnvironmentVariableCredentialsProvider();
    }

    /**
     * Create an instance of EnvironmentVariableCredentialsProvider by reading
     * the java system property used when starting up the JVM to enable the
     * default metrics collected by the TableStore SDK, such as -Dtablestore.accessKeyId and
     * -Dtablestore.accessKeySecret.
     * 
     * @return A {@link SystemPropertiesCredentialsProvider} instance.
     * @throws ClientException
     *             TableStore Client side exception.
     */
    public static SystemPropertiesCredentialsProvider newSystemPropertiesCredentialsProvider() throws ClientException {
        return new SystemPropertiesCredentialsProvider();
    }

    /**
     * Create an instance of InstanceProfileCredentialsProvider obtained the
     * ak/sk by ECS Metadata Service.
     * 
     * @param roleName
     *            Role name of the ECS binding, NOT ROLE ARN.
     * @return A {@link InstanceProfileCredentialsProvider} instance.
     * @throws ClientException
     *             TableSTore Client side exception.
     */
    public static InstanceProfileCredentialsProvider newInstanceProfileCredentialsProvider(String roleName)
            throws ClientException {
        return new InstanceProfileCredentialsProvider(roleName);
    }
}