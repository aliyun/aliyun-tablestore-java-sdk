package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.TableStoreCallback;

public interface WatchableCallback<Request, Response>
  extends TableStoreCallback<Request, Response> {
    public WatchableCallback<Request, Response> watchBy(TableStoreCallback<Request, Response> watcher);
}
