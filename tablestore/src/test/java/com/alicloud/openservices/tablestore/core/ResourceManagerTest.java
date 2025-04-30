package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentialProvider;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferCodedInputStream;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ResourceManagerTest {

    private static Logger logger = LoggerFactory.getLogger(ResourceManagerTest.class);

    @Test
    public void testCreateClientWithRM() throws Exception {
        ServiceSettings settings = ServiceSettings.load();
        logger.debug("trigger to create logger thread");

        int initThreadCount = Thread.getAllStackTraces().size();

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        Random random = new Random();
        int ioThread = random.nextInt(5) + 1;
        int retryThread = random.nextInt(3) + 1;
        int callbackThread = random.nextInt(3) + 1;
        clientConfiguration.setIoThreadCount(ioThread);
        clientConfiguration.setRetryThreadCount(retryThread);
        ExecutorService executorService = Executors.newFixedThreadPool(callbackThread);
        ResourceManager resourceManager = new ResourceManager(clientConfiguration, executorService);

        SyncClient syncClient = new SyncClient(settings.getOTSEndpoint(),
                new DefaultCredentialProvider(settings.getOTSAccessKeyId(), settings.getOTSAccessKeySecret()),
                settings.getOTSInstanceName(),
                clientConfiguration, resourceManager.sharedResourceManager());
        for (int i = 0; i < callbackThread; i++) {
            syncClient.listTable();
        }

        int threadCount = Thread.getAllStackTraces().size();
        assertEquals(ioThread + 1 + 1 + callbackThread, threadCount - initThreadCount);

        SyncClient syncClientWithoutShare1 = new SyncClient(settings.getOTSEndpoint(),
                settings.getOTSAccessKeyId(), settings.getOTSAccessKeySecret(),
                settings.getOTSInstanceName());
        for (int i = 0; i < callbackThread; i++) {
            syncClientWithoutShare1.listTable();
        }

        int newThreadCount = Thread.getAllStackTraces().size() - threadCount;
        threadCount += newThreadCount;
        assertTrue(newThreadCount > 0);

        SyncClient syncClientWithoutShare2 = new SyncClient(settings.getOTSEndpoint(),
                settings.getOTSAccessKeyId(), settings.getOTSAccessKeySecret(),
                settings.getOTSInstanceName());
        for (int i = 0; i < callbackThread; i++) {
            syncClientWithoutShare2.listTable();
        }

        assertEquals(newThreadCount, Thread.getAllStackTraces().size() - threadCount);
        threadCount = Thread.getAllStackTraces().size();

        SyncClient syncClientShareResource = new SyncClient(settings.getOTSEndpoint(),
                new DefaultCredentialProvider(settings.getOTSAccessKeyId(), settings.getOTSAccessKeySecret()),
                settings.getOTSInstanceName(),
                clientConfiguration, resourceManager.sharedResourceManager());
        for (int i = 0; i < callbackThread; i++) {
            syncClientShareResource.listTable();
        }

        assertEquals(0, Thread.getAllStackTraces().size() - threadCount);
        syncClientShareResource.shutdown();
        assertEquals(0, Thread.getAllStackTraces().size() - threadCount);

        syncClientWithoutShare2.shutdown();
        assertEquals(newThreadCount, threadCount - Thread.getAllStackTraces().size());

        threadCount -= newThreadCount;
        syncClientWithoutShare1.shutdown();
        assertEquals(newThreadCount, threadCount - Thread.getAllStackTraces().size());

        threadCount -= newThreadCount;
        syncClient.shutdown();
        assertEquals(0, Thread.getAllStackTraces().size() - threadCount);
        assertTrue(Thread.getAllStackTraces().size() > initThreadCount);

        resourceManager.shutdown();
        assertEquals(initThreadCount, Thread.getAllStackTraces().size());
    }
}
