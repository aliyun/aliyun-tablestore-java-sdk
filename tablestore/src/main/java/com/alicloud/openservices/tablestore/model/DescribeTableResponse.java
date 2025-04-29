package com.alicloud.openservices.tablestore.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class DescribeTableResponse extends Response implements Jsonizable {
    /**
     * Table Meta definition.
     */
    private TableMeta tableMeta;

    /**
     * The reserved throughput information of the table.
     */
    private ReservedThroughputDetails reservedThroughputDetails;

    /**
     * Table configuration parameters.
     */
    private TableOptions tableOptions;

    /**
     * The partition information of the table.
     */
    private List<PrimaryKey> shardSplits;

    /**
     * The Stream information of the table.
     */
    private StreamDetails streamDetails;

    /**
     * The server-side encryption information of the table.
     */
    private SSEDetails sseDetails;

    /**
     * Index table information
     */
    private List<IndexMeta> indexMeta = new ArrayList<IndexMeta>();

    /**
     * Table creation time, in microseconds
     */
    private long creationTime;

    /*
     * Internal interface. Do not use.
     */
    public DescribeTableResponse(Response meta) {
        super(meta);
    }

    /**
     * Returns the table's Meta definition.
     *
     * @return The table's Meta.
     */
    public TableMeta getTableMeta() {
        return tableMeta;
    }

    /*
     * Internal interface. Do not use.
     */
    public void setTableMeta(TableMeta tableMeta) {
        this.tableMeta = tableMeta;
    }

    /**
     * Returns the change information of the table's reserved throughput.
     *
     * @return The change information of the table's reserved throughput.
     */
    public ReservedThroughputDetails getReservedThroughputDetails() {
        return reservedThroughputDetails;
    }

    /**
     * The split points between all shards.
     * Assume a table has two pk columns and three shards. Then two single-column split points, A and B, are returned.
     * The ranges of the three shards are (-inf,-inf) to (A,-inf), (A,-inf) to (B,-inf), (B,-inf) to (+inf,+inf).
     */
    public List<PrimaryKey> getShardSplits() {
        return shardSplits;
    }

    public void setShardSplits(List<PrimaryKey> splits) {
        shardSplits = splits;
    }

    /*
     * Internal interface. Do not use.
     */
    public void setReservedThroughputDetails(ReservedThroughputDetails reservedThroughputDetails) {
        this.reservedThroughputDetails = reservedThroughputDetails;
    }

    /**
     * Get the table's configuration parameters.
     *
     * @return The table's configuration parameters.
     */
    public TableOptions getTableOptions() {
        return tableOptions;
    }

    /**
     * Get the Stream information of the table.
     *
     * @return The Stream information of the table.
     */
    public StreamDetails getStreamDetails() {
        return streamDetails;
    }

    public void setStreamDetails(StreamDetails streamDetails) {
        this.streamDetails = streamDetails;
    }

    /**
     * Get the server-side encryption information of the table.
     *
     * @return The server-side encryption information of the table.
     */
    public SSEDetails getSseDetails() {
        return sseDetails;
    }

    public void setSseDetails(SSEDetails sseDetails) {
        this.sseDetails = sseDetails;
    }

    /**
     * Get the information of the index table
     *
     * @return Information of the index table
     */
    public List<IndexMeta> getIndexMeta() {
        return Collections.unmodifiableList(indexMeta);
    }

    public void addIndexMeta(IndexMeta indexMeta) {
        this.indexMeta.add(indexMeta);
    }

    public void setTableOptions(TableOptions tableOptions) {
        this.tableOptions = tableOptions;
    }

    /**
     * Get the table creation time, in microseconds.
     *
     * @return long Table creation time
     * */
    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
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
        sb.append("\"CreationTime\": ");
        sb.append(creationTime);
        sb.append(",");
        sb.append(newline);
        sb.append("\"TableMeta\": ");
        tableMeta.jsonize(sb, newline + "  ");
        sb.append(",");
        sb.append(newline);
        sb.append("\"ReservedThroughputDetails\": ");
        reservedThroughputDetails.jsonize(sb, newline + "  ");
        sb.append(",");
        sb.append(newline);
        sb.append("\"TableOptionsEx\": ");
        tableOptions.jsonize(sb, newline + "  ");
        sb.append(",");
        sb.append(newline);
        sb.append("\"IndexMeta\": [");
        boolean first = true;
        for (IndexMeta index : indexMeta) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
                sb.append(newline + " ");
            }
            index.jsonize(sb, newline + " ");
        }
        sb.append("]}");
    }
}
