package com.alicloud.openservices.tablestore.timestream;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.TableStoreWriter;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.timestream.internal.MetaCacheManager;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import com.alicloud.openservices.tablestore.timestream.model.*;
import com.alicloud.openservices.tablestore.timestream.model.query.DataGetter;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 数据表操作类，提供数据的读写api
 */
public class TimestreamDataTable {
    /**
     * TableStore异步client
     */
    private AsyncClient asyncClient;
    /**
     * 数据表名
     */
    private String dataTableName;
    /**
     * Meta表名
     */
    private String metaTable;
    /**
     * Meta表的索引名
     */
    private String index;
    /**
     * 数据表对应的TableStoreWriter
     */
    private TableStoreWriter dataWriter;

    /**
     * 后台meta更新管理器
     */
    private MetaCacheManager metaCacheManager;

    protected TimestreamDataTable(
            AsyncClient asyncClient, String dataTableName, String metaTable, String index, TableStoreWriter dataWriter, MetaCacheManager metaCacheManager) {
        this.asyncClient = asyncClient;
        this.dataTableName = dataTableName;
        this.metaTable = metaTable;
        this.index = index;
        this.dataWriter = dataWriter;
        this.metaCacheManager = metaCacheManager;
    }

    protected void close() {
        this.dataWriter.close();
    }

    private void writeIdentifier(TimestreamIdentifier identifier) {
        if (this.metaCacheManager != null) {
            this.metaCacheManager.addTimestreamMeta(identifier, TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        }
    }

    /**
     * 同步写入数据，如果数据写入失败会抛异常
     * @param identifier 时间线标示
     * @param point 数据点
     */
    public void write(TimestreamIdentifier identifier, Point point) {
        RowPutChange rowChange = Utils.serializeTimestream(this.dataTableName, identifier, point);
        PutRowRequest request = new PutRowRequest();
        request.setRowChange(rowChange);
        Future<PutRowResponse> future = this.asyncClient.putRow(request, null);
        Utils.waitForFuture(future);
        writeIdentifier(identifier);
    }

    /**
     * 异步写入数据，如果写入失败需要通过{@link com.alicloud.openservices.tablestore.TableStoreCallback}来获取
     * <p>异步接口，通过{@link TableStoreWriter}写入数据，相比同步接口，该接口数据写入的吞吐更好</p>
     * 注意：若TableStoreWriter的缓冲区满，则该操作会被block
     * @param identifier 时间线标示
     * @param point 数据点
     */
    public void asyncWrite(TimestreamIdentifier identifier, Point point) {
        this.dataWriter.addRowChange(
                Utils.serializeTimestream(
                        this.dataTableName, identifier, point));
        writeIdentifier(identifier);
    }

    /**
     * 查询单条时间线的数据
     * @param identifier 时间线标示
     * @return
     */
    public DataGetter get(TimestreamIdentifier identifier) {
        return new DataGetter(asyncClient, dataTableName, identifier);
    }

    /**
     * 将内存中缓存的所有数据写入到数据库中
     */
    public void flush() {
        dataWriter.flush();
    }
}
