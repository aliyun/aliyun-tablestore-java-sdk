package com.alicloud.openservices.tablestore.tunnel.worker;

public enum TunnelWorkerStatus {
    /**
     * TunnelWorker的初始状态
     */
    WORKER_READY,
    /**
     * TunnelWorker已启动
     */
    WORKER_STARTED,
    /**
     * TunnelWorker已结束, 比如TunnelWorker被shutdown。
     * 此状态下的TunnelWorker可以通过重新的connectAndWorking恢复正常状态。
     */
    WORKER_ENDED,

    /**
     * TunnelWorker已停止, 比如TunnelWorker中对应的Tunnel被删除。
     * 此状态下的TunnelWorker不能通过重新的connectAndWorking恢复正常状态，需要进行参数的订正等手工修复。
     */
    WORKER_HALT
}
