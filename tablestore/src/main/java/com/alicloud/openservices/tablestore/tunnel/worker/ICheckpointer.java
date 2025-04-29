package com.alicloud.openservices.tablestore.tunnel.worker;

public interface ICheckpointer {
    void checkpoint(String token);
}
