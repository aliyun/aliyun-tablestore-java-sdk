package com.alicloud.openservices.tablestore.timestream;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.timestream.internal.MetaCacheManager;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import com.alicloud.openservices.tablestore.timestream.model.AttributeIndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link TimestreamDBClient}定义，提供删建表，以及数据读写功能。
 * <p>后台默认打开自动更新时间线updateTime功能，可以通过{@link TimestreamDBConfiguration#enableDumpMeta}选择关闭 </p>
 * <p>当后台打开自动更新时间线updateTime功能时（{@link TimestreamDBConfiguration#enableDumpMeta}），TimestreamDBClient后台会维护一个内存缓存最近更新过的时间线。数据写入时，会判断该时间线是否需要更新updateTime（缓存中没有或者上次更新时间线超过设置的阈值），
 * 如果需要更新则往meta表中插入一条记录（只更新updateTime）。</p>
 * <p>数据写入的异步api是通过{@link TableStoreWriter}来实现的，如果需要获取异步写入的结果，可以传入{@link TableStoreCallback}，该callback是所有数据表写入共用的</p>
 */
public class TimestreamDBClient implements TimestreamDB {
    private Logger logger = LoggerFactory.getLogger(TimestreamDBClient.class);

    private TimestreamDBConfiguration config;
    private WriterConfig writerConfig;

    private String metaTableName;
    private String indexName;

    private ExecutorService executor;

    private AsyncClient asyncClient;
    private TableStoreWriter metaWriter;
    private MetaCacheManager metaCacheManager;
    private Map<String, TimestreamDataTable> dataTableMap = new HashMap<String, TimestreamDataTable>();
    private TableStoreCallback<RowChange, ConsumedCapacity> callback;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private boolean writeMeta;

    /**
     * TimestreamDBClient的构造函数
     * @param asyncClient TableStore异步client
     * @param config client配置
     */
    public TimestreamDBClient(AsyncClient asyncClient, TimestreamDBConfiguration config) {
        this(asyncClient, config, new WriterConfig(), null);
    }

    /**
     * TimestreamDBClient的构造函数
     * @param asyncClient TableStore异步client
     * @param config client配置
     * @param writerConfig 所有数据表使用的TableStoreWriter的{@link WriterConfig}
     * @param callback 所有数据表使用的TableStoreWriter共用的{@link TableStoreCallback}
     */
    public TimestreamDBClient(
            AsyncClient asyncClient,
            TimestreamDBConfiguration config,
            WriterConfig writerConfig,
            TableStoreCallback<RowChange, ConsumedCapacity> callback) {
        this.asyncClient = asyncClient;
        this.config = config;
        this.writerConfig = writerConfig;
        this.callback = callback;
        this.metaTableName = this.config.getMetaTableName();
        this.indexName = this.metaTableName + "_INDEX";
        this.writeMeta = config.getDumpMeta();

        executor = Executors.newFixedThreadPool(this.config.getThreadNumForWriter());
        try {
            tryInitMetaWriter();
        } catch (TableStoreException e) {
            logger.warn("Failed to init meta writer:" + e.getMessage());
        } catch (ClientException e) {
            logger.warn("Failed to init meta writer:" + e.toString());
        }
        logger.info("End initialize client");
    }

    @Override
    public synchronized void close() {
        if (closed.get()) {
            throw new ClientException("The client has already been closed.");
        }
        if (this.metaCacheManager != null) {
            this.metaCacheManager.close();
        }
        if (this.metaWriter != null) {
            this.metaWriter.close();
        }
        for (TimestreamDataTable dataTable : this.dataTableMap.values()) {
            dataTable.close();
        }
        this.asyncClient.shutdown();
        this.executor.shutdown();
        closed.set(true);
    }

    private void tryInitMetaWriter() {
        if (!this.writeMeta) {
            return;
        }
        if (this.metaWriter == null) {
            synchronized (this) {
                if (this.metaWriter == null) {
                    logger.info("Begin to init meta writer");
                    checkMetaTableExist();
                    checkIndexMetaExist();
                    this.metaWriter = new DefaultTableStoreWriter(
                            this.asyncClient,
                            this.metaTableName,
                            this.writerConfig, null,
                            executor);
                    this.metaCacheManager = new MetaCacheManager(
                            this.metaTableName,
                            this.config.getIntervalDumpMeta(TimeUnit.SECONDS),
                            this.config.getMetaCacheSize(),
                            this.metaWriter);
                    logger.info("End to init meta writer");
                }
            }
        }
    }

    @Override
    public void createMetaTable() {
        this.createMetaTable(null);
    }

    @Override
    public void createMetaTable(List<AttributeIndexSchema> indexForAttributes) {
        if (indexForAttributes != null) {
            for (AttributeIndexSchema schema : indexForAttributes) {
                String name = schema.getFieldName();
                if (name.equals(TableMetaGenerator.CN_PK0) ||
                        name.equals(TableMetaGenerator.CN_PK1) ||
                        name.equals(TableMetaGenerator.CN_PK2) ||
                        name.equals(TableMetaGenerator.CN_TAMESTAMP_NAME)) {
                    throw new ClientException("Name of attribute for indexes cannot be " +
                            TableMetaGenerator.CN_PK0 + "/" +
                            TableMetaGenerator.CN_PK1 + "/" +
                            TableMetaGenerator.CN_PK2 + "/" +
                            TableMetaGenerator.CN_TAMESTAMP_NAME + ".");
                }
            }
        }
        TableMeta tableMeta = TableMetaGenerator.getMetaTableMeta(this.metaTableName);
        TableOptions tableOptions = new TableOptions();
        CreateTableRequest request = new CreateTableRequest(
                tableMeta, tableOptions);
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(-1);
        Future<CreateTableResponse> res = this.asyncClient.createTable(request, null);
        Utils.waitForFuture(res);
        createSearchIndexForMeta(indexForAttributes);

        tryInitMetaWriter();
    }

    private void createSearchIndexForMeta(List<AttributeIndexSchema> indexForAttributes) {
        CreateSearchIndexRequest request = new CreateSearchIndexRequest(
                this.metaTableName, // 设置表名
                this.indexName); // 设置索引名
        IndexSchema indexSchema = getIndexSchema(indexForAttributes);
        // set name as index routing key
        IndexSetting indexSetting = new IndexSetting();
        indexSetting.setRoutingFields(Arrays.asList(TableMetaGenerator.CN_PK1));
        indexSchema.setIndexSetting(indexSetting);
        request.setIndexSchema(indexSchema);

        Future<CreateSearchIndexResponse> future = this.asyncClient.createSearchIndex(request, null);
        Utils.waitForFuture(future);
    }

    private IndexSchema getIndexSchema(List<AttributeIndexSchema> indexForAttributes) {
        IndexSchema indexSchema = new IndexSchema();
        List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        if (indexForAttributes != null) {
            for (AttributeIndexSchema schema : indexForAttributes) {
                fieldSchemas.add(schema.getFieldSchema());
            }
        }
        fieldSchemas.add(
                new FieldSchema(TableMetaGenerator.CN_PK0, FieldType.KEYWORD));
        fieldSchemas.add(
                new FieldSchema(TableMetaGenerator.CN_PK1, FieldType.KEYWORD).setIndex(true));
        fieldSchemas.add(
                new FieldSchema(TableMetaGenerator.CN_PK2, FieldType.KEYWORD).setIndex(true).setIsArray(true));
        fieldSchemas.add(
                new FieldSchema(TableMetaGenerator.CN_TAMESTAMP_NAME, FieldType.LONG).setIndex(true).setStore(true));
        indexSchema.setFieldSchemas(fieldSchemas);
        return indexSchema;
    }

    @Override
    public void deleteMetaTable() {
        deleteSearchIndexForMeta();
        DeleteTableRequest request = new DeleteTableRequest(this.metaTableName);
        Future<DeleteTableResponse> res = this.asyncClient.deleteTable(request, null);
        Utils.waitForFuture(res);
    }

    private void deleteSearchIndexForMeta() {
        DeleteSearchIndexRequest request = new DeleteSearchIndexRequest();
        request.setTableName(this.metaTableName);
        request.setIndexName(indexName);
        Future<DeleteSearchIndexResponse> future = this.asyncClient.deleteSearchIndex(request, null);
        Utils.waitForFuture(future);
    }

    private void checkMetaTableExist() {
        DescribeTableRequest request = new DescribeTableRequest(this.metaTableName);
        DescribeTableResponse resp = Utils.waitForFuture(this.asyncClient.describeTable(request, null));
        TableMeta tableMeta = resp.getTableMeta();
        TableMeta tableMetaExpect = TableMetaGenerator.getMetaTableMeta(this.metaTableName);
        List<PrimaryKeySchema> pks = tableMeta.getPrimaryKeyList();
        List<PrimaryKeySchema> pksExpect = tableMetaExpect.getPrimaryKeyList();
        if (pks.size() != pksExpect.size()) {
            throw new ClientException("Same table with different meta exist: " + this.metaTableName);
        }
        for (int i =0; i < pks.size(); ++i) {
            if (!pks.get(i).equals(pksExpect.get(i))) {
                throw new ClientException("Same table with different meta exist: " + this.metaTableName);
            }
        }
    }

    private void checkIndexMetaExist() {
        ListSearchIndexRequest request = new ListSearchIndexRequest();
        request.setTableName(this.metaTableName);
        ListSearchIndexResponse resp = Utils.waitForFuture(this.asyncClient.listSearchIndex(request, null));
        List<SearchIndexInfo> indexInfos = resp.getIndexInfos();
        if (indexInfos.size() == 0) {
            throw new ClientException(String.format("Index for meta(%s) not exist: %s", this.metaTableName, this.indexName));
        }
        for (SearchIndexInfo indexInfo : indexInfos) {
            if (indexInfo.getIndexName().equals(this.indexName)) {
                return;
            }
        }
        throw new ClientException(String.format("Index for meta(%s) not exist: %s", this.metaTableName, this.indexName));
    }

    @Override
    public void createDataTable(String tableName) {
        TableMeta tableMeta = TableMetaGenerator.getDataTableMeta(tableName);
        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(-1);
        CreateTableRequest request = new CreateTableRequest(
                tableMeta, tableOptions);
        Future<CreateTableResponse> res = this.asyncClient.createTable(request, null);
        Utils.waitForFuture(res);
    }

    @Override
    public void deleteDataTable(String tableName) {
        DeleteTableRequest request = new DeleteTableRequest(tableName);
        Future<DeleteTableResponse> res = this.asyncClient.deleteTable(request, null);
        Utils.waitForFuture(res);
    }

    @Override
    public synchronized TimestreamDataTable dataTable(String tableName) {
        tryInitMetaWriter();
        TimestreamDataTable dataTable = dataTableMap.get(tableName);
        if (dataTable == null) {
            if (dataTableMap.size() >= config.getMaxDataTableNumForWrite()) {
                throw new ClientException("Number of data table for writen in db cannot be larger than " + config.getMaxDataTableNumForWrite());
            }
            DefaultTableStoreWriter writer = new DefaultTableStoreWriter(
                    asyncClient,
                    tableName,
                    writerConfig,
                    callback,
                    executor
            );
            dataTable = new TimestreamDataTable(
                    asyncClient,
                    tableName,
                    metaTableName,
                    indexName,
                    writer,
                    metaCacheManager
            );
            dataTableMap.put(tableName, dataTable);
        }
        return dataTable;
    }

    @Override
    public TimestreamMetaTable metaTable() {
        tryInitMetaWriter();
        TimestreamMetaTable metaTable = new TimestreamMetaTable(
                asyncClient,
                metaTableName,
                indexName,
                metaCacheManager
        );
        return metaTable;
    }
}
