package com.alicloud.openservices.tablestore.core;

import java.util.List;
import java.util.LinkedList;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

abstract class AbstractWatchableCallback<Request, Response>
  implements WatchableCallback<Request, Response> {

    protected List<TableStoreCallback<Request, Response>> downstreams =
        new LinkedList<TableStoreCallback<Request, Response>>();

    public AbstractWatchableCallback<Request, Response> watchBy(TableStoreCallback<Request, Response> downstream) {
        Preconditions.checkNotNull(downstream, "downstream must not be null.");

        this.downstreams.add(downstream);
        return this;
    }
}
