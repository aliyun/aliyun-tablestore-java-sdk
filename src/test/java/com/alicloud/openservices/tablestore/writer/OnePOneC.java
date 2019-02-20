package com.alicloud.openservices.tablestore.writer;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class OnePOneC {
    private static int queueSize = 4096;
    private static int count = 1;

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

    public static class WorkThread implements Runnable {

        public LongEventProducer producer;

        @Override
        public void run() {
            Executor executor = Executors.newCachedThreadPool();
            // The factory for the event
            LongEventFactory factory = new LongEventFactory();

            // Specify the size of the ring buffer, must be power of 2.
            int bufferSize = queueSize;

            // Construct the Disruptor
            //Disruptor<LongEvent> disruptor = new Disruptor<LongEvent>(factory, bufferSize, executor, ProducerType.MULTI, new BusySpinWaitStrategy());
            Disruptor<LongEvent> disruptor = new Disruptor<LongEvent>(factory, bufferSize, executor);

            final RingBuffer ringBuffer = disruptor.getRingBuffer();
            disruptor.handleEventsWith(new LongEventHandler(0, 0));

            disruptor.start();

            final List<LongEventProducer> ps = new ArrayList<LongEventProducer>();
            List<Thread> ts = new ArrayList<Thread>();
            for (int i = 0; i < 1; i++) {
                producer = new LongEventProducer(ringBuffer);
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

            for (Thread th : ts) {
                th.start();
            }

            for (Thread th : ts) {
                try {
                    th.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            count = Integer.parseInt(args[0]);
        }
        System.out.println("ThreadCount: " + count);

        final WorkThread[] wts = new WorkThread[count];
        final List<Thread> ts = new ArrayList<Thread>();

        for (int i = 0; i < count; i++) {
            wts[i] = new WorkThread();
            Thread th = new Thread(wts[i]);
            ts.add(th);
        }

        for (Thread t : ts) {
            t.start();
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
                    for (WorkThread p : wts) {
                        totalCount += p.producer.count.getAndSet(0);
                    }
                    System.out.println(totalCount);
                }
            }
        });

        qps.start();
        qps.join();
    }
}
