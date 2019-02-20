package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

/**
 * UpdateTable操作的返回结果，包含预留吞吐量的变更细节以及更改之后表的当前配置参数。
 */
public class UpdateTableResponse extends Response implements Jsonizable {
    /**
     * 表当前的预留吞吐量的变更细节信息。
     */
    private ReservedThroughputDetails reservedThroughputDetails;

    /**
     * 表的当前配置参数。
     */
    private TableOptions tableOptions;

    /**
     * 表的Stream信息。
     */
    private StreamDetails streamDetails;

    public UpdateTableResponse(Response meta) {
        super(meta);
    }

    /**
     * 获取表当前的预留吞吐量的更改信息。
     *
     * @return 表当前的预留吞吐量的更改信息。
     */
    public ReservedThroughputDetails getReservedThroughputDetails() {
        return reservedThroughputDetails;
    }

    public void setReservedThroughputDetails(
        ReservedThroughputDetails reservedThroughputDetails)
    {
        this.reservedThroughputDetails = reservedThroughputDetails;
    }

    /**
     * 获取表当前的配置参数。
     *
     * @return 表的配置参数。
     */
    public TableOptions getTableOptions() {
        return tableOptions;
    }

    public void setTableOptions(TableOptions tableOptions) {
        this.tableOptions = tableOptions;
    }

    /**
     * 获取表的Stream信息。
     * @return 表的Stream信息。
     */
    public StreamDetails getStreamDetails() {
        return streamDetails;
    }

    public void setStreamDetails(StreamDetails streamDetails) {
        this.streamDetails = streamDetails;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        sb.append(newline);
        sb.append("\"ReservedThroughputDetails\": ");
        reservedThroughputDetails.jsonize(sb, newline + "  ");
        sb.append(",");
        sb.append(newline);
        sb.append("\"TableOptionsEx\": ");
        tableOptions.jsonize(sb, newline + "  ");
        sb.append(",");
        sb.append(newline);
        sb.append("\"StreamDetails\": ");
        streamDetails.jsonize(sb, newline + "  ");
        sb.append('}');
    }
}
