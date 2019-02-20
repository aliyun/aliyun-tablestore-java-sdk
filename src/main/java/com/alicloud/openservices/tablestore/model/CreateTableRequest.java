package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * CreateTableRequest包含创建一张新的表所必需的一些参数，包括表的Meta、预留读写吞吐量、表的配置以及预分区配置等。
 * 在初始化实例之后，可以通过调用{@link #setTableMeta(TableMeta)} 来更改表的Meta。
 * 在初始化实例之后，可以通过调用{@link #setReservedThroughput(ReservedThroughput)} 来更改表的预留吞吐量。
 * 在初始化实例之后，可以通过调用{@link #setTableOptions(TableOptions)} 来更改表的配置。
 */
public class CreateTableRequest implements Request {
    /**
     * 表的结构信息。
     */
    private TableMeta tableMeta;

    /**
     * 索引表信息
     */
    private List<IndexMeta> indexMeta = new ArrayList<IndexMeta>();

    /**
     * 表的预留吞吐量设置。
     */
    private ReservedThroughput reservedThroughput;

    /**
     * 表的配置项, 包括TTL和最大版本数等。
     */
    private TableOptions tableOptions;

    /**
     * 表的Stream配置。
     */
    private OptionalValue<StreamSpecification> streamSpecification = new OptionalValue<StreamSpecification>("StreamSpecification");

    /**
     * 初始化CreateTableRequest实例。
     * <p>表的预留吞吐量和表的配置都会采用默认值，若有需求需要定制更改，可以调用相应的设置函数。
     * <p>表默认将不进行任何预切分，若需要对表的分区进行设置，可以调用相应的设置函数。
     *
     * @param tableMeta 表的结构信息。
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
     * 获取表的结构信息。
     *
     * @return 表的结构信息
     */
    public TableMeta getTableMeta() {
        return tableMeta;
    }

    /**
     * 设置表的结构信息。
     *
     * @param tableMeta 表的结构信息
     */
    public void setTableMeta(TableMeta tableMeta) {
        Preconditions.checkNotNull(tableMeta, "TableMeta should not be null.");
        this.tableMeta = tableMeta;
    }

    /**
     * 获取表的预留吞吐量。
     *
     * @return 表的预留吞吐量设置。
     */
    public ReservedThroughput getReservedThroughput() {
        return reservedThroughput;
    }

    /**
     * 设置表的预留吞吐量。
     *
     * @param reservedThroughput 表的预留吞吐量。
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
     * 获取表的配置参数。
     *
     * @return 表的配置。
     */
    public TableOptions getTableOptions() {
        return tableOptions;
    }

    /**
     * 设置表的配置参数。
     *
     * @param tableOptions 表的配置。
     */
    public void setTableOptions(TableOptions tableOptions) {
        Preconditions.checkNotNull(tableOptions, "TableOptionsEx should not be null.");
        this.tableOptions = tableOptions;
    }

    /**
     * 获取Stream的配置参数
     *
     * @return Stream的配置参数。若返回null，则代表未设置该配置。
     */
    public StreamSpecification getStreamSpecification() {
        return streamSpecification.getValue();
    }

    /**
     * 设置Stream的配置参数
     *
     * @param streamSpecification
     */
    public void setStreamSpecification(StreamSpecification streamSpecification) {
        Preconditions.checkArgument(streamSpecification != null, "The stream specification should not be null");
        this.streamSpecification.setValue(streamSpecification);
    }

    /**
     * 添加索引表
     *
     * @param indexMetas 索引表meta
     */
    public void addIndex(IndexMeta[] indexMetas) {
        Preconditions.checkArgument(indexMetas != null && indexMetas.length != 0, "The index meta should not be null or empty");
        Collections.addAll(indexMeta, indexMetas);
    }

    /**
     * 添加索引表
     *
     * @param indexMeta 索引表meta
     */
    public void addIndex(IndexMeta indexMeta) {
        Preconditions.checkArgument(indexMeta != null, "The index meta should not be null");
        this.indexMeta.add(indexMeta);
    }

    /**
     * 返回索引表meta列表
     *
     * @return 包含所有索引表meta的只读列表。
     */
    public List<IndexMeta> getIndexMetaList() {
        return Collections.unmodifiableList(indexMeta);
    }
}
