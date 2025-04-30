package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.Row;

import java.util.Iterator;

public interface ITablestoreSplit {

    /**
     * @param client
     */
    void initial(SyncClient client);

    /**
     * @param client
     * @return
     */
    Iterator<Row> getRowIterator(SyncClientInterface client);

    Iterator<Row> getRowIterator(SyncClientInterface client, FilterPushdownConfig filterPushdownConfig);
}
