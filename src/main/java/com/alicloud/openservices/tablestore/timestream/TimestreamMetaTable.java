package com.alicloud.openservices.tablestore.timestream;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.TableStoreWriter;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.timestream.internal.MetaCacheManager;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamMeta;
import com.alicloud.openservices.tablestore.timestream.model.filter.Filter;
import com.alicloud.openservices.tablestore.timestream.model.query.MetaGetter;
import com.alicloud.openservices.tablestore.timestream.model.query.MetaFilter;

import java.util.concurrent.Future;

/**
 * Meta表操作类，提供meta的读写api
 */
public class TimestreamMetaTable {
    /**
     * TableStore异步client
     */
    private AsyncClient asyncClient;
    /**
     * Meta表名
     */
    private String metaTableName;
    /**
     * Meta表的索引名
     */
    private String index;
    /**
     * 后台meta更新管理器
     */
    private MetaCacheManager metaCacheManager;

    protected TimestreamMetaTable(AsyncClient asyncClient, String metaTableName, String index, MetaCacheManager metaCacheManager) {
        this.asyncClient = asyncClient;
        this.metaTableName = metaTableName;
        this.index = index;
        this.metaCacheManager = metaCacheManager;
    }

    /**
     * 删除某条时间线
     * @param identifier 时间线标示
     */
    public void delete(TimestreamIdentifier identifier) {
        RowDeleteChange rowChange = Utils.serializeTimestreamMeta(this.metaTableName, identifier);
        DeleteRowRequest request = new DeleteRowRequest();
        request.setRowChange(rowChange);
        Future<DeleteRowResponse> future = this.asyncClient.deleteRow(request, null);
        Utils.waitForFuture(future);
    }

    /**
     * 插入一条时间线
     * @param meta 时间线
     */
    public void put(TimestreamMeta meta) {
        if (this.metaCacheManager != null) {
            this.metaCacheManager.updateTimestreamMeta(meta.getIdentifier(), meta.getUpdateTimeInUsec());
        }

        RowPutChange rowChange = Utils.serializeTimestreamMeta(this.metaTableName, meta);
        PutRowRequest request = new PutRowRequest();
        request.setRowChange(rowChange);
        Future<PutRowResponse> future = this.asyncClient.putRow(request, null);
        Utils.waitForFuture(future);
    }

    /**
     * 查询某条时间线
     * @param identifier 时间线标示
     * @return
     */
    public MetaGetter get(TimestreamIdentifier identifier) {
        return new MetaGetter(asyncClient, metaTableName, identifier);
    }

    /**
     * 查询所有时间线
     * @return
     */
    public MetaFilter filter() {
        return new MetaFilter(asyncClient, metaTableName, index, null);
    }

    /**
     * 查询满足条件的时间线
     * @param filter 查询条件
     * @return
     */
    public MetaFilter filter(Filter filter) {
        return new MetaFilter(asyncClient, metaTableName, index, filter);
    }
}
