package com.alicloud.openservices.tablestore.writer;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class RingBufferPerf {
    private static int threadCount = 5;
    private static int consumerCount = 1;
    private static int queueSize = 4096;
    private static AtomicLong globalCount = new AtomicLong();

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
        private final RingBuffer<LongEvent> ringBuffer;

        public LongEventProducer(RingBuffer<LongEvent> ringBuffer) {
            this.ringBuffer = ringBuffer;
        }

        public void onData(long value) {
            long sequence = ringBuffer.next();  // Grab the next sequence
            try {
                LongEvent event = ringBuffer.get(sequence); // Get the entry in the Disruptor
                // for the sequence
                event.set(value);  // Fill with data
            } finally {
                ringBuffer.publish(sequence);
            }
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
            globalCount.incrementAndGet();
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

        Executor executor = Executors.newFixedThreadPool(5);
        // The factory for the event
        LongEventFactory factory = new LongEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = queueSize;

        // Construct the Disruptor
        Disruptor<LongEvent> disruptor = new Disruptor<LongEvent>(factory, bufferSize, executor);

        RingBuffer ringBuffer = disruptor.getRingBuffer();
        for (int i = 0; i < consumerCount; i++) {
            disruptor.handleEventsWith(new LongEventHandler(consumerCount, i));
        }

        disruptor.start();

        final LongEventProducer producer = new LongEventProducer(ringBuffer);

        List<Thread> ts = new ArrayList<Thread>();
        for (int i = 0; i < threadCount; i++) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        //System.out.println("Put data: " + s);
                        producer.onData(globalCount.incrementAndGet());
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

                    System.out.println(globalCount.getAndSet(0));
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
