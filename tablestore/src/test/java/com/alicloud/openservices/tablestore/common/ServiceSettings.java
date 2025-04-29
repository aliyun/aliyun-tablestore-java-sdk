package com.alicloud.openservices.tablestore.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.Properties;

public class ServiceSettings {

    private static final String SETTINGS_FILE_NAME = "src/test/resources/conf.properties";
    private static final String LOCAL_SETTINGS_FILE_NAME = "src/test/resources/local_conf.properties";

    private static final Log log = LogFactory.getLog(ServiceSettings.class);

    private Properties properties = new Properties();


    public ServiceSettings() {
    }

    public String getOTSEndpoint() {
        String endpoint =  properties.getProperty("ots.endpoint");
        if (endpoint.isEmpty()) {
            endpoint = System.getenv("endpoint");
        }
        return endpoint;
    }

    public void setOTSEndpoint(String otsEndpoint) {
        properties.setProperty("ots.endpoint", otsEndpoint);
    }

    public String getOTSAccessKeyId() {
        String accessKeyId =  properties.getProperty("ots.accesskeyid");
        if (accessKeyId.isEmpty()) {
            accessKeyId = System.getenv("accesskeyid");
        }
        return accessKeyId;
    }

    public void setOTSAccessKeyId(String otsAccessKeyId) {
        properties.setProperty("ots.accesskeyid", otsAccessKeyId);
    }

    public String getOTSAccessKeySecret() {
        String accessKeySecret =  properties.getProperty("ots.accesskeysecret");
        if (accessKeySecret.isEmpty()) {
            accessKeySecret = System.getenv("accesskeysecret");
        }
        return accessKeySecret;
    }

    public void setOTSAccessKeySecret(String otsAccessKeySecret) {
        properties.setProperty("ots.accesskeysecret", otsAccessKeySecret);
    }

    public String getOTSInstanceName() {
        String instanceName =  properties.getProperty("ots.instancename");
        if (instanceName.isEmpty()) {
            instanceName = System.getenv("instancename");
        }
        return instanceName;
    }

    public void setOTSInstanceName(String otsInstanceName) {
        properties.setProperty("ots.instancename", otsInstanceName);
    }

    public void setOTSInstanceNameInternal(String otsInstanceName) {
        properties.setProperty("ots.instancenameinternal", otsInstanceName);
    }

    public String getOTSInstanceNameInternal() {
        String instanceName =  properties.getProperty("ots.instancenameinternal");
        if (instanceName.isEmpty()) {
            instanceName = System.getenv("instancenameinternal");
        }
        return instanceName;
    }

    public String getProxyHost() {
        return properties.getProperty("proxy.host");
    }

    public void setProxyHost(String proxyHost) {
        properties.setProperty("proxy.host", proxyHost);
    }

    public int getProxyPort() {
        if (properties.getProperty("proxy.port") != null) {
            return Integer.parseInt(properties.getProperty("proxy.port"));
        }
        else {
            return 0;
        }
    }

    public void setProxyPort(int proxyPort) {
        properties.setProperty("proxy.port", String.valueOf(proxyPort));
    }

    /**
     * Load settings from the configuration file.
     * <p>
     * The configuration format:
     * ots.endpoint=
     * ots.accesskeyid=
     * ots.accesskeysecret=
     * ots.instancename=
     * proxy.host=
     * proxy.port=
     * </p>
     * @return
     */
    public static ServiceSettings load(String settingFile) {
        ServiceSettings ss = new ServiceSettings();

        InputStream is = null;
        try {
            is = new FileInputStream(settingFile);
            Properties pr = new Properties();
            pr.load(is);

            ss.properties = pr;

        } catch (FileNotFoundException e) {
            log.warn("The settings file '" + settingFile + "' does not exist.");
        } catch (IOException e) {
            log.warn("Failed to load the settings from the file: " + settingFile);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) { }
            }
        }

        return ss;
    }

    public static ServiceSettings load() {
        if (new File(LOCAL_SETTINGS_FILE_NAME).isFile()) {
            return load(LOCAL_SETTINGS_FILE_NAME);
        } else {
            return load(SETTINGS_FILE_NAME);
        }
    }
}

