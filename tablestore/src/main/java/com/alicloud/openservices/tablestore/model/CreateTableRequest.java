package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * CreateTableRequest contains some necessary parameters for creating a new table, including the table's Meta, reserved read/write throughput, table configuration, and pre-partition configuration.
 * After initializing the instance, you can change the table's Meta by calling {@link #setTableMeta(TableMeta)}.
 * After initializing the instance, you can change the table's reserved throughput by calling {@link #setReservedThroughput(ReservedThroughput)}.
 * After initializing the instance, you can change the table's configuration by calling {@link #setTableOptions(TableOptions)}.
 */
public class CreateTableRequest implements Request {
    /**
     * The structure information of the table.
     */
    private TableMeta tableMeta;

    /**
     * Index table information
     */
    private List<IndexMeta> indexMeta = new ArrayList<IndexMeta>();

    /**
     * The reserved throughput setting for the table.
     */
    private ReservedThroughput reservedThroughput;

    /**
     * Table configuration items, including TTL and maximum number of versions, etc.
     */
    private TableOptions tableOptions;

    /**
     * The Stream configuration of the table.
     */
    private OptionalValue<StreamSpecification> streamSpecification = new OptionalValue<StreamSpecification>("StreamSpecification");

    /**
     * The server-side encryption configuration for the table.
     */
    private OptionalValue<SSESpecification> sseSpecification = new OptionalValue<SSESpecification>("SSESpecification");

    /**
     * Whether to enable local transactions
     */
    private OptionalValue<Boolean> enableLocalTxn = new OptionalValue<Boolean>("EnableLocalTxn");

    /**
     * Initializes a CreateTableRequest instance.
     * <p>The table's reserved throughput and configuration will both use default values. If there is a need for customization, the corresponding setter functions can be called.
     * <p>By default, the table will not perform any pre-splitting. If you need to configure the table partitions, you can call the corresponding setting function.
     *
     * @param tableMeta The structural information of the table.
     */
    public CreateTableRequest(TableMeta tableMeta, TableOptions tableOptions) {
        this(tableMeta, tableOptions, new ReservedThroughput());
    }

    public CreateTableRequest(
            TableMeta tableMeta,
            TableOptions tableOptions,
            ReservedThroughput reservedThroughput) {
        setTableOptions(tableOptions);
        setReservedThroughput(reservedThroughput);
        setTableMeta(tableMeta);
    }

    public CreateTableRequest(
            TableMeta tableMeta,
            TableOptions tableOptions,
            List<IndexMeta> indexMeta) {
        setTableMeta(tableMeta);
        setTableOptions(tableOptions);
        setReservedThroughput(new ReservedThroughput());
        for (IndexMeta index : indexMeta) {
            addIndex(index);
        }
    }

    public CreateTableRequest(
            TableMeta tableMeta,
            TableOptions tableOptions,
            ReservedThroughput reservedThroughput,
            List<IndexMeta> indexMeta) {
        setTableMeta(tableMeta);
        setTableOptions(tableOptions);
        setReservedThroughput(reservedThroughput);
        for (IndexMeta index : indexMeta) {
            addIndex(index);
        }
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_TABLE;
    }

    /**
     * Get the table structure information.
     *
     * @return Table structure information
     */
    public TableMeta getTableMeta() {
        return tableMeta;
    }

    /**
     * Set the table's structure information.
     *
     * @param tableMeta the table's structure information
     */
    public void setTableMeta(TableMeta tableMeta) {
        Preconditions.checkNotNull(tableMeta, "TableMeta should not be null.");
        Preconditions.checkArgument(tableMeta.getPrimaryKeyList().size() != 0,
                "TableMeta should set at least one primary key.");
        this.tableMeta = tableMeta;
    }

    /**
     * Get the reserved throughput of the table.
     *
     * @return The reserved throughput setting of the table.
     */
    public ReservedThroughput getReservedThroughput() {
        return reservedThroughput;
    }

    /**
     * Set the reserved throughput for the table.
     *
     * @param reservedThroughput The reserved throughput for the table.
     */
    public void setReservedThroughput(ReservedThroughput reservedThroughput) {
        Preconditions.checkNotNull(reservedThroughput, "ReservedThroughput should not be null.");
        CapacityUnit cu = reservedThroughput.getCapacityUnit();
        Preconditions.checkArgument(cu.hasSetReadCapacityUnit() && cu.hasSetWriteCapacityUnit(),
                "You must set both read and write capacity unit.");
        Preconditions.checkArgument(cu.getReadCapacityUnit() >= 0, "The value of read capacity unit must be greater than 0.");
        Preconditions.checkArgument(cu.getWriteCapacityUnit() >= 0, "The value of write capacity unit must be greater than 0.");
        this.reservedThroughput = reservedThroughput;
    }

    /**
     * Get the configuration parameters of the table.
     *
     * @return The configuration of the table.
     */
    public TableOptions getTableOptions() {
        return tableOptions;
    }

    /**
     * Set the table configuration parameters.
     *
     * @param tableOptions The table configuration.
     */
    public void setTableOptions(TableOptions tableOptions) {
        Preconditions.checkNotNull(tableOptions, "TableOptionsEx should not be null.");
        this.tableOptions = tableOptions;
    }

    /**
     * Get the configuration parameters of the Stream.
     *
     * @return The configuration parameters of the Stream. If it returns null, it means that this configuration has not been set.
     */
    public StreamSpecification getStreamSpecification() {
        return streamSpecification.getValue();
    }

    /**
     * Set the configuration parameters for the Stream
     *
     * @param streamSpecification
     */
    public void setStreamSpecification(StreamSpecification streamSpecification) {
        Preconditions.checkArgument(streamSpecification != null, "The stream specification should not be null");
        this.streamSpecification.setValue(streamSpecification);
    }

    /**
     * Get the configuration parameters for server-side encryption.
     *
     * @return The configuration parameters for server-side encryption. If null is returned, it means that this configuration has not been set.
     */
    public SSESpecification getSseSpecification() {
        return sseSpecification.getValue();
    }

    /**
     * Set the configuration parameters for server-side encryption
     *
     * @param sseSpecification
     */
    public void setSseSpecification(SSESpecification sseSpecification) {
        Preconditions.checkArgument(sseSpecification != null, "The server-side-encryption specification should not be null");
        this.sseSpecification.setValue(sseSpecification);
    }

    /**
     * Add index table
     *
     * @param indexMetas Index table meta
     */
    public void addIndex(IndexMeta[] indexMetas) {
        Preconditions.checkArgument(indexMetas != null && indexMetas.length != 0, "The index meta should not be null or empty");
        Collections.addAll(indexMeta, indexMetas);
    }

    /**
     * Add index table
     *
     * @param indexMeta Index table meta
     */
    public void addIndex(IndexMeta indexMeta) {
        Preconditions.checkArgument(indexMeta != null, "The index meta should not be null");
        this.indexMeta.add(indexMeta);
    }

    /**
     * Returns the index table meta list.
     *
     * @return A read-only list containing all index table metas.
     */
    public List<IndexMeta> getIndexMetaList() {
        return Collections.unmodifiableList(indexMeta);
    }

    /**
     * Whether the local transaction switch is explicitly set
     *
     * @return Whether the local transaction switch is set.
     */
    public boolean hasLocalTxnSet() {
        return enableLocalTxn.isValueSet();
    }

    /**
     * Set the local transaction switch
     *
     * @param enableLocalTxn Local transaction switch
     */
    public void setLocalTxnEnabled(boolean enableLocalTxn) {
        this.enableLocalTxn.setValue(enableLocalTxn);
    }

    /**
     * Get the local transaction switch. If it is not set, an exception will be thrown.
     *
     * @return The setting of the local transaction switch.
     */
    public boolean isLocalTxnEnabled() {
        if (!enableLocalTxn.isValueSet()) {
            throw new IllegalStateException("The value of enableLocalTxn is not set.");
        }
        return enableLocalTxn.getValue();
    }
}
