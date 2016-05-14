package com.aliyun.openservices.ots.integration;

import static org.junit.Assert.*;

import com.aliyun.openservices.ots.model.DeleteTableRequest;
import com.aliyun.openservices.ots.model.ListTableResult;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.OTSServiceConfiguration;
import com.aliyun.openservices.ots.internal.OTSRetryStrategy;
import com.aliyun.openservices.ots.utils.ServiceSettings;

import java.util.logging.Logger;

public class OTSRetryStrategyTest {
    private static int retries = 0;
    private static final OTS ots = OTSClientFactory.createOTSClient(
            ServiceSettings.load(), new ClientConfiguration());
    private static Logger LOG = Logger.getLogger(OTSRetryStrategyTest.class.getName());

    public static void setRetries(int retries) {
        OTSRetryStrategyTest.retries = retries;
    }

    @Before
    public void setup() throws Exception {
        ListTableResult r = ots.listTable();

        for (String table: r.getTableNames()) {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest(table);
            ots.deleteTable(deleteTableRequest);

            LOG.info("Delete table: " + table);
            Thread.sleep(1000);
        }
    }

    @Test
    public void testNoRetry() {
        OTSRetryStrategy retryStrategy = new OTSRetryStrategy() {
            @Override
            public boolean shouldRetry(String action, Exception ex, int retries) {
                OTSRetryStrategyTest.setRetries(retries);
                return false;
            }

            @Override
            public long getPauseDelay(String action, Exception ex, int retries) {
                return 10;
            }
        };
        ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(10);
        config.setIoThreadCount(10);
        OTSServiceConfiguration serviceConfig = new OTSServiceConfiguration();
        serviceConfig.setRetryStrategy(retryStrategy);
        OTS ots = OTSClientFactory.createOTSClient(
                ServiceSettings.load(), config, serviceConfig);
        OTSRetryStrategyTest.setRetries(0);
        ots.listTable();
        assertEquals(0, retries);
    }
    
    @Test
    public void testRetryOTSException() {
        OTSRetryStrategy retryStrategy = new OTSRetryStrategy() {
            @Override
            public boolean shouldRetry(String action, Exception ex, int retries) {
                OTSRetryStrategyTest.setRetries(retries);
                if ((ex instanceof OTSException) && (retries < 3)) {
                    return true;
                }
                return false;
            }

            @Override
            public long getPauseDelay(String action, Exception ex, int retries) {
                return 10;
            }
        };
        ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(10);
        config.setIoThreadCount(10);
        OTSServiceConfiguration serviceConfig = new OTSServiceConfiguration();
        serviceConfig.setRetryStrategy(retryStrategy);
        ServiceSettings ss = ServiceSettings.load();
        ss.setOTSAccessKeyId("WrongAccessId");
        OTS ots = OTSClientFactory.createOTSClient(
                ss, config, serviceConfig);
        OTSRetryStrategyTest.setRetries(0);
        try {
            ots.listTable();
        } catch (OTSException ex) {
            
        }
        assertEquals(3, retries);
    }
    
    @Test
    public void testRetryClientException() {
        OTSRetryStrategy retryStrategy = new OTSRetryStrategy() {
            @Override
            public boolean shouldRetry(String action, Exception ex, int retries) {
                OTSRetryStrategyTest.setRetries(retries);
                if ((ex instanceof ClientException) && (retries < 3)) {
                    return true;
                }
                return false;
            }

            @Override
            public long getPauseDelay(String action, Exception ex, int retries) {
                return 10;
            }
        };
        ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(10);
        config.setIoThreadCount(10);
        config.setConnectionTimeoutInMillisecond(1000);
        OTSServiceConfiguration serviceConfig = new OTSServiceConfiguration();
        serviceConfig.setRetryStrategy(retryStrategy);
        ServiceSettings ss = ServiceSettings.load();
        ss.setOTSEndpoint("http://1.1.1.1");
        OTS ots = OTSClientFactory.createOTSClient(
                ss, config, serviceConfig);
        OTSRetryStrategyTest.setRetries(0);
        try {
            ots.listTable();
        } catch (ClientException ex) {
            
        }
        assertEquals(3, retries);
    }

}
