package com.alicloud.openservices.tablestore.timeserieswriter.callback;

import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesGroup;

import java.util.List;


public interface TimeseriesCallbackFactory {

    /**
     * When supporting batch management, pass in the group instance for each row.
     * Actively update batch statistics after the callback request is completed.
     */
    TableStoreCallback newInstance(List<TimeseriesGroup> groupFuture);
}
