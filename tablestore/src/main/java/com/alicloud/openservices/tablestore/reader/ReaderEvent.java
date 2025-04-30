package com.alicloud.openservices.tablestore.reader;

import java.util.concurrent.CountDownLatch;

import com.lmax.disruptor.EventFactory;

public class ReaderEvent {
    public EventType type;
    public PrimaryKeyWithTable pkWithTable;
    public ReaderGroup readerGroup;
    public CountDownLatch latch;
    public ReaderEvent() {
    }

    public void setValue(PrimaryKeyWithTable pkWithTable, final ReaderGroup readerGroup) {
        this.type = null;
        this.pkWithTable = pkWithTable;
        this.readerGroup = readerGroup;
    }

    public void setValue(CountDownLatch latch, EventType type) {
        this.type = type;
        this.latch = latch;
    }

    public enum EventType {
        FLUSH,
        SEND,
        QUERY
    }

    public static class ReaderEventFactory implements EventFactory<ReaderEvent> {
        @Override
        public ReaderEvent newInstance() {
            return new ReaderEvent();
        }
    }
}
