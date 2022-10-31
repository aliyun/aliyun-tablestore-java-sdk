package com.aliyun.openservices.ots.internal.writer;

import com.aliyun.openservices.ots.model.RowChange;
import com.lmax.disruptor.EventFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class RowChangeEvent {
    public static enum EventType {
        DATA, FLUSH
    }

    public EventType type;
    public RowChange rowChange;
    public CountDownLatch latch;

    private RowChangeEvent() {

    }

    public void setValue(RowChange rowChange) {
        this.type = EventType.DATA;
        this.rowChange = rowChange;
    }

    public void setValue(CountDownLatch latch) {
        this.type = EventType.FLUSH;
        this.latch = latch;
    }

    public static class RowChangeEventFactory implements EventFactory<RowChangeEvent> {
        @Override
        public RowChangeEvent newInstance() {
            return new RowChangeEvent();
        }
    }
}
