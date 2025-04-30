package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

/**
 * The return result of the UpdateTable operation, which includes the details of the reserved throughput changes and the current configuration parameters of the table after the change.
 */
public class UpdateTableResponse extends Response implements Jsonizable {
    /**
     * The change details of the table's current reserved throughput.
     */
    private ReservedThroughputDetails reservedThroughputDetails;

    /**
     * The current configuration parameters of the table.
     */
    private TableOptions tableOptions;

    /**
     * The Stream information of the table.
     */
    private StreamDetails streamDetails;

    public UpdateTableResponse(Response meta) {
        super(meta);
    }

    /**
     * Get the change information of the current reserved throughput of the table.
     *
     * @return The change information of the current reserved throughput of the table.
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
     * Get the current configuration parameters of the table.
     *
     * @return The configuration parameters of the table.
     */
    public TableOptions getTableOptions() {
        return tableOptions;
    }

    public void setTableOptions(TableOptions tableOptions) {
        this.tableOptions = tableOptions;
    }

    /**
     * Get the Stream information of the table.
     * @return The Stream information of the table.
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
