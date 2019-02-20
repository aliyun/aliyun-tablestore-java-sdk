package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * UpdateTableRequest包含更新一张表所必需的一些参数，包括表的名称、预留吞吐量更改和表的配置更改等。
 * <p>用户通过UpdateTable可以单独对预留吞吐量进行更改或者单独对表部分配置项进行更改，或者一起。</p>
 */
public class UpdateTableRequest implements Request {

    /**
     * 表的名称。
     */
    private String tableName;

    /**
     * 表的预留吞吐量变更。
     * 可以单独更改读能力单元或者写能力单元。
     */
    private ReservedThroughput reservedThroughputForUpdate;

    /**
     * 表的配置参数选项。
     */
    private TableOptions tableOptionsForUpdate;

    /**
     * 表的Stream配置变更。
     */
    private StreamSpecification streamSpecification;

    public UpdateTableRequest() {
    }

    public UpdateTableRequest(String tableName) {
        setTableName(tableName);
    }

    /**
     * 获取表的名称。
     *
     * @return 表的名称。
     */
    public String getTableName() {
        Preconditions.checkNotNull(tableName);
        return tableName;
    }

    /**
     * 设置表的名称
     *
     * @param tableName 表的名称
     */
    public void setTableName(String tableName) {
        Preconditions.checkNotNull(tableName, "tableName must not be null.");
        Preconditions.checkArgument(!tableName.isEmpty(),
            "The name of table must not be empty.");
        this.tableName = tableName;
    }

    /**
     * 获取表的预留吞吐量变更。
     *
     * @return 表的预留吞吐量变更。
     */
    public ReservedThroughput getReservedThroughputForUpdate() {
        return reservedThroughputForUpdate;
    }

    /**
     * 设置表的预留吞吐量变更。
     *
     * @param reservedThroughputForUpdate 表的预留吞吐量更改。
     */
    public void setReservedThroughputForUpdate(ReservedThroughput reservedThroughputForUpdate) {
        this.reservedThroughputForUpdate = reservedThroughputForUpdate;
    }

    /**
     * 获取表的参数更改。
     *
     * @return 表的参数更改。
     */
    public TableOptions getTableOptionsForUpdate() {
        return tableOptionsForUpdate;
    }

    /**
     * 设置表的参数更改。
     *
     * @param tableOptionsForUpdate 表的参数更改。
     */
    public void setTableOptionsForUpdate(TableOptions tableOptionsForUpdate) {
        this.tableOptionsForUpdate = tableOptionsForUpdate;
    }

    public String getOperationName() {
        return OperationNames.OP_UPDATE_TABLE;
    }

    /**
     * 获取表的Stream配置变更。
     *
     * @return 表的Stream配置变更。
     */
    public StreamSpecification getStreamSpecification() {
        return streamSpecification;
    }

    /**
     * 设置表的Stream配置变更。
     *
     * @param streamSpecification 表的Stream配置变更。
     */
    public void setStreamSpecification(StreamSpecification streamSpecification) {
        this.streamSpecification = streamSpecification;
    }
}
