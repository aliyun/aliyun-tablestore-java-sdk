package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.TableStoreCallback;

import java.util.List;

/**
 * Get the Callback instance: supports two types of Callback implementations, namely in-order and concurrent.
 */
public interface CallbackFactory {

    /**
     * When supporting batch management, pass in the group instance of each row
     * Actively update batch statistics after the callback ends the request
     */
    TableStoreCallback newInstance(List<Group> groupFuture);
}
