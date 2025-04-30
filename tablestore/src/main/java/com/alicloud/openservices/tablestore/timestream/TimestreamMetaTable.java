package com.alicloud.openservices.tablestore.timestream;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.timestream.internal.MetaCacheManager;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamMeta;
import com.alicloud.openservices.tablestore.timestream.model.query.MetaGetter;
import com.alicloud.openservices.tablestore.timestream.model.query.MetaFilter;
import com.alicloud.openservices.tablestore.timestream.model.filter.Filter;

import java.util.concurrent.Future;

/**
 * Meta table operation class, providing read and write APIs for meta
 */
public class TimestreamMetaTable {
    /**
     * TableStore asynchronous client
     */
    private AsyncClient asyncClient;
    /**
     * Meta table name
     */
    private String metaTableName;
    /**
     * The index name of the Meta table
     */
    private String index;
    /**
     * Background meta update manager
     */
    private MetaCacheManager metaCacheManager;

    protected TimestreamMetaTable(AsyncClient asyncClient, String metaTableName, String index, MetaCacheManager metaCacheManager) {
        this.asyncClient = asyncClient;
        this.metaTableName = metaTableName;
        this.index = index;
        this.metaCacheManager = metaCacheManager;
    }

    /**
     * Delete a specific timeline
     * @param identifier The identifier of the timeline
     */
    public void delete(TimestreamIdentifier identifier) {
        RowDeleteChange rowChange = Utils.serializeTimestreamMetaToDelete(this.metaTableName, identifier);
        DeleteRowRequest request = new DeleteRowRequest();
        request.setRowChange(rowChange);
        Future<DeleteRowResponse> future = this.asyncClient.deleteRow(request, null);
        Utils.waitForFuture(future);
    }

    /**
     * Insert a timeline
     * @param meta Timeline
     */
    public void put(TimestreamMeta meta) {
        if (this.metaCacheManager != null) {
            this.metaCacheManager.updateTimestreamMeta(meta.getIdentifier(), meta.getUpdateTimeInUsec());
        }

        RowPutChange rowChange = Utils.serializeTimestreamMetaToPut(this.metaTableName, meta);
        PutRowRequest request = new PutRowRequest();
        request.setRowChange(rowChange);
        Future<PutRowResponse> future = this.asyncClient.putRow(request, null);
        Utils.waitForFuture(future);
    }

    /**
     * Update a timeline
     * @param meta Timeline metadata
     */
    public void update(TimestreamMeta meta) {
        if (this.metaCacheManager != null) {
            this.metaCacheManager.updateTimestreamMeta(meta.getIdentifier(), meta.getUpdateTimeInUsec());
        }

        RowUpdateChange rowChange = Utils.serializeTimestreamMetaToUpdate(this.metaTableName, meta);
        UpdateRowRequest request = new UpdateRowRequest();
        request.setRowChange(rowChange);
        Future<UpdateRowResponse> future = this.asyncClient.updateRow(request, null);
        Utils.waitForFuture(future);
    }

    /**
     * Query a specific timeline
     * @param identifier The identifier of the timeline
     * @return
     */
    public MetaGetter get(TimestreamIdentifier identifier) {
        return new MetaGetter(asyncClient, metaTableName, identifier);
    }

    /**
     * Query all timelines
     * @return
     */
    public MetaFilter filter() {
        return new MetaFilter(asyncClient, metaTableName, index, null);
    }

    /**
     * Query the timeline that satisfies the conditions
     * @param filter Query conditions
     * @return
     */
    public MetaFilter filter(Filter filter) {
        return new MetaFilter(asyncClient, metaTableName, index, filter);
    }
}
