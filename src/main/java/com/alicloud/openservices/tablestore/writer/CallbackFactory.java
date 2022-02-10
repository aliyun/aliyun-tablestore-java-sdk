package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.TableStoreCallback;

import java.util.List;

/**
 * 获取Callback实例：支持按序、并发两种Callback实现
 */
public interface CallbackFactory {

    /**
     * 支持批量管理时，传入每行的群组实例
     * Callback结束请求后主动更新批量统计
     */
    TableStoreCallback newInstance(List<Group> groupFuture);
}
