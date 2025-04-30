package com.alicloud.openservices.tablestore.writer.dispatch;

import com.alicloud.openservices.tablestore.model.RowChange;


public interface Dispatcher {
    /**
     * Get the write partition bucket number
     */
    int getDispatchIndex(RowChange rowChange);
}
