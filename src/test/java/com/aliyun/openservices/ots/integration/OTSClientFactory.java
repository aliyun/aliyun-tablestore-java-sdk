package com.aliyun.openservices.ots.integration;

import static com.aliyun.openservices.ots.utils.CodingUtils.isNullOrEmpty;

import java.util.concurrent.ExecutorService;

import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.OTSAsync;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.OTSClientAsync;
import com.aliyun.openservices.ots.OTSServiceConfiguration;
import com.aliyun.openservices.ots.utils.ServiceSettings;

public class OTSClientFactory {
    
    public static OTS createOTSClient() {
        return createOTSClient(ServiceSettings.load());
    }

    public static OTS createOTSClient(ServiceSettings ss){
        ClientConfiguration config = new ClientConfiguration();

        // Use specified endpoint and config.
        // Set max conn to 1 to detect CLOSE_WAIT bugs.
        config.setMaxConnections(1);
        if (!isNullOrEmpty(ss.getProxyHost())) {
            config.setProxyHost(ss.getProxyHost());
            config.setProxyPort(ss.getProxyPort());
        }

        return createOTSClient(ss, config);
    }

    public static OTS createOTSClient(ServiceSettings ss, ClientConfiguration config){
        String otsEndpoint = ss.getOTSEndpoint();
        String accessKeyId = ss.getOTSAccessKeyId();
        String accessKeySecret = ss.getOTSAccessKeySecret();
        String instanceName = ss.getOTSInstanceName();
        OTSServiceConfiguration osc = new OTSServiceConfiguration();
        osc.setEnableResponseContentMD5Checking(true);

        return new OTSClient(otsEndpoint, accessKeyId, accessKeySecret, instanceName, config, osc);
    }
    
    public static OTS createOTSClient(ServiceSettings ss, ClientConfiguration config, OTSServiceConfiguration serviceConfig){
        String otsEndpoint = ss.getOTSEndpoint();
        String accessKeyId = ss.getOTSAccessKeyId();
        String accessKeySecret = ss.getOTSAccessKeySecret();
        String instanceName = ss.getOTSInstanceName();

        return new OTSClient(otsEndpoint, accessKeyId, accessKeySecret, instanceName, config, serviceConfig);
    }

    public static OTSAsync createOTSClientAsync() {
        return createOTSClientAsync(ServiceSettings.load());
    }

    public static OTSAsync createOTSClientAsync(ServiceSettings ss){
        ClientConfiguration config = new ClientConfiguration();
        if (!isNullOrEmpty(ss.getProxyHost())) {
            config.setProxyHost(ss.getProxyHost());
            config.setProxyPort(ss.getProxyPort());
        }
        return createOTSClientAsync(ss, config);
    }
    
    public static OTSAsync createOTSClientAsync(ServiceSettings ss, ClientConfiguration config){
        String otsEndpoint = ss.getOTSEndpoint();
        String accessKeyId = ss.getOTSAccessKeyId();
        String accessKeySecret = ss.getOTSAccessKeySecret();
        String instanceName = ss.getOTSInstanceName();
        OTSServiceConfiguration osc = new OTSServiceConfiguration();
        osc.setEnableResponseContentMD5Checking(true);
        return new OTSClientAsync(otsEndpoint, accessKeyId, accessKeySecret, instanceName, config, osc, null);
    }
    
    public static OTSAsync createOTSClientAsync(ServiceSettings ss, ClientConfiguration config, ExecutorService pool){
        String otsEndpoint = ss.getOTSEndpoint();
        String accessKeyId = ss.getOTSAccessKeyId();
        String accessKeySecret = ss.getOTSAccessKeySecret();
        String instanceName = ss.getOTSInstanceName();
        OTSServiceConfiguration osc = new OTSServiceConfiguration();
        osc.setEnableResponseContentMD5Checking(true);
        return new OTSClientAsync(otsEndpoint, accessKeyId, accessKeySecret, instanceName, config, osc, pool);
    }
    
    public static OTSAsync createOTSClientAsync(ServiceSettings ss, ClientConfiguration config, OTSServiceConfiguration serviceConfig){
        String otsEndpoint = ss.getOTSEndpoint();
        String accessKeyId = ss.getOTSAccessKeyId();
        String accessKeySecret = ss.getOTSAccessKeySecret();
        String instanceName = ss.getOTSInstanceName();
        return new OTSClientAsync(otsEndpoint, accessKeyId, accessKeySecret, instanceName, config, serviceConfig, null);
    }
    
    public static OTSAsync createOTSClientAsync(ServiceSettings ss, ClientConfiguration config, OTSServiceConfiguration serviceConfig, ExecutorService pool){
        String otsEndpoint = ss.getOTSEndpoint();
        String accessKeyId = ss.getOTSAccessKeyId();
        String accessKeySecret = ss.getOTSAccessKeySecret();
        String instanceName = ss.getOTSInstanceName();
        return new OTSClientAsync(otsEndpoint, accessKeyId, accessKeySecret, instanceName, config, serviceConfig, pool);
    }

}