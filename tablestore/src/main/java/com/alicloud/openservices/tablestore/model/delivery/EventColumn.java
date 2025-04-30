package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;


public class EventColumn {

    /**
     * Column name for the event time
     */
    private String columnName;

    /**
     * Event time column time format
     */
    private EventTimeFormat timeFormat;

    /**
     * Initialize the EventColumn instance
     *
     * @param columnName Column name
     * @param timeFormat Time format
     */
    public EventColumn(String columnName, EventTimeFormat timeFormat ) {
        setColumnName(columnName);
        setEventTimeFormat(timeFormat);
    }

    /**
     * Get the column name of the event time column
     *
     * @return the column name of the event time column
     */
    public String getColumnName() { return columnName; }

    /**
     * Set the column name for the event time column
     *
     * @param columnName The column name for the event time column
     */
    public void setColumnName(String columnName) {
        Preconditions.checkArgument(columnName != null && !columnName.isEmpty(), "the columnName should not be null or empty");
        this.columnName = columnName;
    }

    /**
     * Get the event time format
     *
     * @return Event time format
     */
    public EventTimeFormat getEventTimeFormat() { return timeFormat; }

    /**
     * Set the event time format
     *
     * @param timeFormat The time format of the event time column
     */
    public void setEventTimeFormat(EventTimeFormat timeFormat) {
        Preconditions.checkArgument(timeFormat != null,
                "The time format should not be null.");
        this.timeFormat = timeFormat;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("columnName: " + columnName + ", " );
        sb.append("timeFormat: " + timeFormat.name() + ", ");
        return sb.toString();
    }
}
