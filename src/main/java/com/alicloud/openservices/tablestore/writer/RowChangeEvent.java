package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.model.RowChange;
import com.lmax.disruptor.EventFactory;

import java.util.concurrent.CountDownLatch;

public class RowChangeEvent {
    public enum EventType {
        DATA, FLUSH
    }

    public EventType type;
    public RowChange rowChange;
    public CountDownLatch latch;
    public Group group;

    private RowChangeEvent() {

    }

    public void setValue(RowChange rowChange, Group group) {
        this.type = EventType.DATA;
        this.rowChange = rowChange;
        this.group = group;
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
