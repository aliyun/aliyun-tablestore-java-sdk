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
 * Data table operation class, providing data read and write APIs
 */
public class TimestreamDataTable {
    /**
     * TableStore asynchronous client
     */
    private AsyncClient asyncClient;
    /**
     * Table name
     */
    private String dataTableName;
    /**
     * Meta table name
     */
    private String metaTable;
    /**
     * Index name of the Meta table
     */
    private String index;
    /**
     * TableStoreWriter corresponding to the data table
     */
    private TableStoreWriter dataWriter;

    /**
     * Background meta update manager
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
     * Synchronously write data, and if the data writing fails, an exception will be thrown.
     * @param identifier Timeline identifier
     * @param point Data point
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
     * Asynchronously write data, if the write fails, it needs to be obtained through {@link com.alicloud.openservices.tablestore.TableStoreCallback}
     * <p>Asynchronous interface, writes data via {@link TableStoreWriter}. Compared to synchronous interfaces, this interface offers better throughput for data writing.</p>
     * Note: If the buffer of TableStoreWriter is full, this operation will be blocked.
     * @param identifier Timeline identifier
     * @param point Data point
     */
    public void asyncWrite(TimestreamIdentifier identifier, Point point) {
        this.dataWriter.addRowChange(
                Utils.serializeTimestream(
                        this.dataTableName, identifier, point));
        writeIdentifier(identifier);
    }

    /**
     * Query a single timeline data
     * @param identifier The timeline identifier
     * @return
     */
    public DataGetter get(TimestreamIdentifier identifier) {
        return new DataGetter(asyncClient, dataTableName, identifier);
    }

    /**
     * Write all the data cached in memory to the database
     */
    public void flush() {
        dataWriter.flush();
    }
}
