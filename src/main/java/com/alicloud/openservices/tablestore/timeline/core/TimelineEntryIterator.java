package com.alicloud.openservices.tablestore.timeline.core;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.RangeIteratorParameter;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.iterator.GetRangeRowIterator;
import com.alicloud.openservices.tablestore.model.iterator.RowIterator;
import com.alicloud.openservices.tablestore.timeline.model.TimelineEntry;
import com.alicloud.openservices.tablestore.timeline.model.TimelineSchema;
import com.alicloud.openservices.tablestore.timeline.utils.Utils;

import java.util.Iterator;

public class TimelineEntryIterator implements Iterator<TimelineEntry> {
    private RowIterator rowIterator;
    private TimelineSchema schema;

    TimelineEntryIterator(SyncClientInterface client, RangeIteratorParameter iteratorParameter, TimelineSchema schema) {
        rowIterator = new GetRangeRowIterator(client, iteratorParameter);
        this.schema = schema;
    }

    @Override
    public boolean hasNext() {
        return rowIterator.hasNext();
    }

    @Override
    public TimelineEntry next() {
        Row row = rowIterator.next();
        return Utils.rowToTimelineEntry(schema, row);
    }

    @Override
    public void remove() {
        rowIterator.remove();
    }
}
