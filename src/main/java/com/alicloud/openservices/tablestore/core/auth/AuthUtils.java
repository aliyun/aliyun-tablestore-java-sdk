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

import java.io.*;

public class AuthUtils {

    /**
     * Default expiration time
     */
    public static final int DEFAULT_EXPIRED_DURATION_SECONDS = 3600;

    /**
     * Default expiration time adjustment factor
     */
    public static final double DEFAULT_EXPIRED_FACTOR = 0.8;

    /**
     * The maximum number of retries when getting AK/SK from ECS
     */
    public static final int MAX_ECS_METADATA_FETCH_RETRY_TIMES = 3;

    /**
     * AK/SK expiration time obtained from ECS Metadata Service, default 6 hours
     */
    public static final int DEFAULT_ECS_SESSION_TOKEN_DURATION_SECONDS = 3600 * 6;

    /**
     * AK/SK expire time obtained from STS, default 1 hour
     */
    public static final int DEFAULT_STS_SESSION_TOKEN_DURATION_SECONDS = 3600 * 1;

    /**
     * Connection timeout when getting AK/SK, the default 5 seconds
     */
    public static final int DEFAULT_HTTP_SOCKET_TIMEOUT_IN_MILLISECONDS = 5000;

    /**
     * Environment variable name for the tablestore access key ID
     */
    public static final String ACCESS_KEY_ENV_VAR = "TABLESTORE_ACCESS_KEY_ID";

    /**
     * Environment variable name for the tablestore secret key
     */
    public static final String SECRET_KEY_ENV_VAR = "TABLESTORE_ACCESS_KEY_SECRET";

    /**
     * Environment variable name for the tablestore session token
     */
    public static final String SESSION_TOKEN_ENV_VAR = "TABLESTORE_SESSION_TOKEN";

    /**
     * System property used when starting up the JVM to enable the default
     * metrics collected by the TableStore SDK.
     *
     * <pre>
     * Example: -Dtablestore.accessKeyId
     * </pre>
     */
    /** System property name for the TableStore access key ID */
    public static final String ACCESS_KEY_SYSTEM_PROPERTY = "tablestore.accessKeyId";

    /** System property name for the TableStore secret key */
    public static final String SECRET_KEY_SYSTEM_PROPERTY = "tablestore.accessKeySecret";

    /** System property name for the TableStore session token */
    public static final String SESSION_TOKEN_SYSTEM_PROPERTY = "tablestore.sessionToken";
}
