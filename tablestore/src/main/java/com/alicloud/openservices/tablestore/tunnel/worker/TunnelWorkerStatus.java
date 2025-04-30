package com.alicloud.openservices.tablestore.tunnel.worker;

public enum TunnelWorkerStatus {
    /**
     * The initial state of TunnelWorker
     */
    WORKER_READY,
    /**
     * TunnelWorker has been started
     */
    WORKER_STARTED,
    /**
     * The TunnelWorker has ended, for example, the TunnelWorker has been shut down.
     * The TunnelWorker in this state can be restored to a normal state by reconnecting and resuming work through connectAndWorking.
     */
    WORKER_ENDED,

    /**
     * The TunnelWorker has stopped, for example, the corresponding Tunnel in TunnelWorker has been deleted.
     * A TunnelWorker in this state cannot be restored to normal status by reconnecting and working, manual repairs such as parameter corrections are required.
     */
    WORKER_HALT
}
