package com.aliyun.openservices.ots.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ServiceSettings {

    private static final String SETTINGS_FILE_NAME = "src/test/resources/conf.properties";

    private static final Log log = LogFactory.getLog(ServiceSettings.class);
    
    private Properties properties = new Properties();


    public ServiceSettings() {
    }

    public String getOTSEndpoint() {
        String endpoint =  properties.getProperty("ots.endpoint");
        if (endpoint.isEmpty()) {
            endpoint = System.getenv("ots.endpoint");
        }
        return endpoint;
    }

    public void setOTSEndpoint(String otsEndpoint) {
        properties.setProperty("ots.endpoint", otsEndpoint);
    }

    public String getOTSAccessKeyId() {
        String accessKeyId =  properties.getProperty("ots.accesskeyid");
        if (accessKeyId.isEmpty()) {
            accessKeyId = System.getenv("ots.accesskeyid");
        }
        return accessKeyId;
    }

    public void setOTSAccessKeyId(String otsAccessKeyId) {
        properties.setProperty("ots.accesskeyid", otsAccessKeyId);
    }

    public String getOTSAccessKeySecret() {
        String accessKeySecret =  properties.getProperty("ots.accesskeysecret");
        if (accessKeySecret.isEmpty()) {
            accessKeySecret = System.getenv("ots.accesskeysecret");
        }
        return accessKeySecret;
    }

    public void setOTSAccessKeySecret(String otsAccessKeySecret) {
        properties.setProperty("ots.accesskeysecret", otsAccessKeySecret);
    }
    
    public String getOTSInstanceName() {
        String instanceName =  properties.getProperty("ots.instancename");
        if (instanceName.isEmpty()) {
            instanceName = System.getenv("ots.instancename");
        }
        return instanceName;
    }
    
    public void setOTSInstanceName(String otsInstanceName) {
        properties.setProperty("ots.instancename", otsInstanceName);
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
    public static ServiceSettings load() {
        ServiceSettings ss = new ServiceSettings();

        InputStream is = null;
        try {
            is = new FileInputStream(SETTINGS_FILE_NAME);
            Properties pr = new Properties();
            pr.load(is);

            ss.properties = pr;

        } catch (FileNotFoundException e) {
            log.warn("The settings file '" + SETTINGS_FILE_NAME + "' does not exist.");
        } catch (IOException e) {
            log.warn("Failed to load the settings from the file: " + SETTINGS_FILE_NAME);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) { }
            }
        }

        return ss;
    }
}
