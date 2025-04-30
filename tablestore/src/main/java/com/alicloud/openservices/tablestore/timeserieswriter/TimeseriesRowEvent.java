package com.alicloud.openservices.tablestore.timeserieswriter;

import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesTableRow;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesGroup;
import com.lmax.disruptor.EventFactory;

import java.util.concurrent.CountDownLatch;

public class TimeseriesRowEvent {
    public enum EventType {
        DATA, FLUSH
    }

    public TimeseriesRowEvent.EventType type;
    public TimeseriesTableRow timeseriesTableRow;
    public CountDownLatch latch;
    public TimeseriesGroup timeseriesGroup;

    private TimeseriesRowEvent() {

    }

    public void setValue(TimeseriesTableRow timeseriesTableRow, TimeseriesGroup timeseriesGroup) {
        this.type = TimeseriesRowEvent.EventType.DATA;
        this.timeseriesTableRow = timeseriesTableRow;
        this.timeseriesGroup = timeseriesGroup;
    }

    public void setValue(CountDownLatch latch) {
        this.type = TimeseriesRowEvent.EventType.FLUSH;
        this.latch = latch;
    }

    public static class TimeseriesRowEventFactory implements EventFactory<TimeseriesRowEvent> {
        @Override
        public TimeseriesRowEvent newInstance() {
            return new TimeseriesRowEvent();
        }
    }
}
