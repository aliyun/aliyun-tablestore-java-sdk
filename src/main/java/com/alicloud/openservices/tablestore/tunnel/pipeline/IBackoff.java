package com.alicloud.openservices.tablestore.tunnel.pipeline;

public interface IBackoff {
    /**
     * 将BackOfff重设为初始状态。
     */
    void reset();

    /**
     * 获取休眠时间。
     * @return 休眠时间
     */
    long nextBackOffMillis();
}
