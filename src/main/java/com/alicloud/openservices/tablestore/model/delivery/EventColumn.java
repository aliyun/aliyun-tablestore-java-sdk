package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;


public class EventColumn {

    /**
     * 事件时间列列名
     */
    private String columnName;

    /**
     * 事件时间列时间格式
     */
    private EventTimeFormat timeFormat;

    /**
     * 初始化EventColumn实例
     *
     * @param columnName 列名
     * @param timeFormat 时间格式
     */
    public EventColumn(String columnName, EventTimeFormat timeFormat ) {
        setColumnName(columnName);
        setEventTimeFormat(timeFormat);
    }

    /**
     * 获取事件时间列列名
     *
     * @return 事件时间列列名
     */
    public String getColumnName() { return columnName; }

    /**
     * 设置事件时间列列名
     *
     * @param columnName 事件时间列列名
     */
    public void setColumnName(String columnName) {
        Preconditions.checkArgument(columnName != null && !columnName.isEmpty(), "the columnName should not be null or empty");
        this.columnName = columnName;
    }

    /**
     * 获取事件时间格式
     *
     * @return 事件时间格式
     */
    public EventTimeFormat getEventTimeFormat() { return timeFormat; }

    /**
     * 设置事件时间格式
     *
     * @param timeFormat 事件时间列时间格式
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
