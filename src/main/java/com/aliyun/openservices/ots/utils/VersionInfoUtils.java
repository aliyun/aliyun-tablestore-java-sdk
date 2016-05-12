/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.utils;

import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class VersionInfoUtils {
    private static final String VERSION_INFO_FILE = "versioninfo.properties";
    
    private static Log log = LogFactory.getLog(VersionInfoUtils.class);

    
    private static String version = null;
    
    private static String defaultUserAgent = null;

    public static String getVersion() {
        if (version == null) {
            initializeVersion();
        }
        return version;
    }
    
    public static String getDefaultUserAgent() {
        if (defaultUserAgent == null) {
            defaultUserAgent = "aliyun-sdk-java" + "/" + getVersion();
        }
        return defaultUserAgent;
    }
    
    private static void initializeVersion() {
        InputStream inputStream = VersionInfoUtils.class.getClassLoader().getResourceAsStream(VERSION_INFO_FILE);
        Properties versionInfoProperties = new Properties();
        try {
            if (inputStream == null)
                throw new Exception(VERSION_INFO_FILE + " not found on classpath");
            
            versionInfoProperties.load(inputStream);
            version = versionInfoProperties.getProperty("version");
        } catch (Exception e) {
            log.info("Unable to load version information for the running SDK: " + e.getMessage());
            version = "unknown-version";
        }
    }
}
