package com.alicloud.openservices.tablestore.writer.dispatch;

import com.alicloud.openservices.tablestore.model.RowChange;


public interface Dispatcher {
    /**
     * 获取写入分桶编号
     * */
    int getDispatchIndex(RowChange rowChange);
}
