package com.alicloud.openservices.tablestore.timeserieswriter.callback;

import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesGroup;

import java.util.List;


public interface TimeseriesCallbackFactory {

    /**
     * 支持批量管理时，传入每行的群组实例
     * Callback结束请求后主动更新批量统计
     */
    TableStoreCallback newInstance(List<TimeseriesGroup> groupFuture);
}
