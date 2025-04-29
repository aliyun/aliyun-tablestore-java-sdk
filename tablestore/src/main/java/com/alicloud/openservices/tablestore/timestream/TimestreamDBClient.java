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
 * Definition of {@link TimestreamDBClient}, providing functionalities for table creation/deletion and data read/write.
 * <p>By default, the backend automatically enables the updateTime timeline update feature, which can be disabled via {@link TimestreamDBConfiguration#dumpMeta}.</p>
 * <p>When the automatic updateTime timeline update feature is enabled (via {@link TimestreamDBConfiguration#dumpMeta}), the TimestreamDBClient backend maintains an in-memory cache of recently updated timelines. During data writes, it determines whether the timeline's updateTime needs to be updated (if not present in the cache or if the last update exceeds the configured threshold). If an update is required, a record is inserted into the meta table (updating only updateTime).</p>
 * <p>The asynchronous API for data writes is implemented through {@link TableStoreWriter}. To retrieve the results of asynchronous writes, you can pass a {@link TableStoreCallback}, which is shared across all data table writes.</p>
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
     * Constructor of TimestreamDBClient
     * @param asyncClient TableStore asynchronous client
     * @param config Client configuration
     */
    public TimestreamDBClient(AsyncClient asyncClient, TimestreamDBConfiguration config) {
        this(asyncClient, config, new WriterConfig(), null);
    }

    /**
     * Constructor of TimestreamDBClient
     * @param asyncClient TableStore asynchronous client
     * @param config client configuration
     * @param writerConfig {@link WriterConfig} for the TableStoreWriter used by all data tables
     * @param callback Shared {@link TableStoreCallback} for the TableStoreWriter used by all data tables
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
                this.metaTableName, // Set the table name
                this.indexName); // Set the index name
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
                new FieldSchema(TableMetaGenerator.CN_PK1, FieldType.KEYWORD).setIndex(true).setEnableSortAndAgg(true));
        fieldSchemas.add(
                new FieldSchema(TableMetaGenerator.CN_PK2, FieldType.KEYWORD).setIndex(true).setIsArray(true).setEnableSortAndAgg(true));
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
