package com.alicloud.openservices.tablestore.tunnel.functiontest;

import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.model.StreamRecord;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorker;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TestMaxChannelParallel {
    private static final String ENDPOINT = "https://vehicle-test.cn-hangzhou.ots.aliyuncs.com";
    private static final String ACCESS_ID = "";
    private static final String ACCESS_KEY = "";
    private static final String INSTANCE_NAME = "vehicle-test";

    static class SimpleProcessor implements IChannelProcessor {
        @Override
        public void process(ProcessRecordsInput input) {
            System.out.println(
                    String.format("Process %d records, NextToken: %s", input.getRecords().size(), input.getNextToken()));
            try {
                // Mock Record Process.
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void shutdown() {
            System.out.println("Mock shutdown");
        }
    }

    protected static ThreadPoolExecutor newDefaultThreadPool(final String threadPrefix, int corePoolSize, int maxCorePoolSize) {
        return new ThreadPoolExecutor(corePoolSize, maxCorePoolSize, 60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(16),
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger();

                    @Override
                    public Thread newThread(Runnable r) {
                        String threadName = threadPrefix + counter.getAndIncrement();
                        System.out.println("SourceHandler new thread: " + threadName);
                        return new Thread(r, threadName);
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    protected static ThreadPoolExecutor newFixedThreadPool(final String threadPrefix, int nThreads) {
        return new ThreadPoolExecutor(nThreads, nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                String threadName = threadPrefix + counter.getAndIncrement();
                System.out.println("SourceHandler new thread: " + threadName);
                return new Thread(r, threadName);
            }
        });
    }

    public static void main(String[] args) {
        TunnelClient client = new TunnelClient(ENDPOINT, ACCESS_ID, ACCESS_KEY, INSTANCE_NAME);
        TunnelWorkerConfig config = new TunnelWorkerConfig(new SimpleProcessor());
        ThreadPoolExecutor readRecordThreadPoolExecutor =
                newDefaultThreadPool("tunnel-read-record",
                        5, 100
                        );
        config.setReadRecordsExecutor(readRecordThreadPoolExecutor);

        ThreadPoolExecutor processRecordThreadPoolExecutor =
                newDefaultThreadPool("tunnel-process-record",
                        5, 100);
        config.setProcessRecordsExecutor(processRecordThreadPoolExecutor);

//        ThreadPoolExecutor channelHelperExcutor =
//                newFixedThreadPool("channel-helper-executor", 5);
//        config.setChannelHelperExecutor(channelHelperExcutor);

        config.setMaxChannelParallel(5);
        TunnelWorker worker1 = new TunnelWorker("70c29c19-50ae-4646-8631-2846e57f670b", client, config);
        try {
            System.out.println("worker running....");
            worker1.connectAndWorking();
        } catch (Exception e) {
            e.printStackTrace();
            worker1.shutdown();
        }
    }
}
