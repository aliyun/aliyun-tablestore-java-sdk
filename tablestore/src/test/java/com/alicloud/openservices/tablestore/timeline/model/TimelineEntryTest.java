package com.alicloud.openservices.tablestore.timeline.model;

import org.junit.Test;
import static org.junit.Assert.*;

public class TimelineEntryTest {

    @Test
    public void testBasic() {
        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("id", "test").build();
        TimelineMessage message = new TimelineMessage();
        long sequenceId = System.currentTimeMillis();
        TimelineEntry entry = new TimelineEntry(sequenceId, message);

        assertEquals(entry.getSequenceID(), sequenceId);
        assertEquals(entry.getMessage(), message);
    }
}
