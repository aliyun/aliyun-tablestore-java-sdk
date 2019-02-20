package com.alicloud.openservices.tablestore.writer;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiPMultiC {
    private static int threadCount = 1;
    private static int consumerCount = 1;
    private static int queueSize = 4096;

    public static class LongEvent {
        private long value;

        public void set(long value) {
            this.value = value;
        }
    }

    public static class LongEventFactory implements EventFactory<LongEvent> {
        public LongEvent newInstance() {
            return new LongEvent();
        }
    }

    public static class LongEventProducer {
        public AtomicInteger count = new AtomicInteger();
        private final RingBuffer<LongEvent> ringBuffer;

        public LongEventProducer(RingBuffer<LongEvent> ringBuffer)
        {
            this.ringBuffer = ringBuffer;
        }

        public void tryPut(long value) {
            while (true) {
                try {
                    long sequence = ringBuffer.tryNext();
                    LongEvent event = ringBuffer.get(sequence);
                    event.set(value);
                    ringBuffer.publish(sequence);
                    return;
                } catch (InsufficientCapacityException e) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException exp) {
                    }
                }
            }
        }

        public void busyPut(long value) {
            long sequence = ringBuffer.next();
            try {
                LongEvent event = ringBuffer.get(sequence);
                event.set(value);
            } finally {
                ringBuffer.publish(sequence);
            }
        }

        public void onData(long value) {
            count.incrementAndGet();
            tryPut(value);
        }
    }

    public static class LongEventHandler implements EventHandler<LongEvent> {
        private int ordinal;
        private int id;

        public LongEventHandler(int ordinal, int id) {
            this.ordinal = ordinal;
            this.id = id;
        }

        public void onEvent(LongEvent event, long sequence, boolean endOfBatch) {

        }
    }

    public static class WorkerHandler implements WorkHandler<LongEvent> {

        @Override
        public void onEvent(LongEvent longEvent) throws Exception {
            //Thread.sleep(1000);
            //System.out.println(this + ":" + longEvent.value);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            threadCount = Integer.parseInt(args[0]);
            consumerCount = Integer.parseInt(args[1]);
        }
        System.out.println("ThreadCount: " + threadCount);
        System.out.println("ConsumerCount: " + consumerCount);

        Executor executor = Executors.newCachedThreadPool();
        // The factory for the event
        LongEventFactory factory = new LongEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = queueSize;

        // Construct the Disruptor
        //Disruptor<LongEvent> disruptor = new Disruptor<LongEvent>(factory, bufferSize, executor, ProducerType.MULTI, new BusySpinWaitStrategy());
        Disruptor<LongEvent> disruptor = new Disruptor<LongEvent>(factory, bufferSize, executor);

        final RingBuffer ringBuffer = disruptor.getRingBuffer();
        EventHandler[] handlers = new EventHandler[consumerCount];
        for (int i = 0; i < consumerCount; i++) {
            handlers[i] = new LongEventHandler(consumerCount, i);
        }
        disruptor.handleEventsWith(handlers);

        disruptor.start();


        final List<LongEventProducer> ps = new ArrayList<LongEventProducer>();
        List<Thread> ts = new ArrayList<Thread>();
        for (int i = 0; i < threadCount; i++) {
            final LongEventProducer producer = new LongEventProducer(ringBuffer);
            ps.add(producer);
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        //System.out.println("Put data: " + s);
                        producer.onData(0);
                    }
                }
            });
            ts.add(th);
        }

        Thread qps = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    long totalCount = 0;
                    for (LongEventProducer p : ps) {
                        totalCount += p.count.getAndSet(0);
                    }
                    System.out.println(totalCount);
                }
            }
        });

        qps.start();

        for (Thread th : ts) {
            th.start();
        }

        qps.join();
    }
}
