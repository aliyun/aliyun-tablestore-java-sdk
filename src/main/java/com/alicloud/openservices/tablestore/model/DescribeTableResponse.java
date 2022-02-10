package com.alicloud.openservices.tablestore.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class DescribeTableResponse extends Response implements Jsonizable {
    /**
     * 表的Meta定义。
     */
    private TableMeta tableMeta;

    /**
     * 表的预留吞吐量的信息。
     */
    private ReservedThroughputDetails reservedThroughputDetails;

    /**
     * 表的配置参数。
     */
    private TableOptions tableOptions;

    /**
     * 表的分区信息。
     */
    private List<PrimaryKey> shardSplits;

    /**
     * 表的Stream信息。
     */
    private StreamDetails streamDetails;

    /**
     * 表的服务器端加密信息。
     */
    private SSEDetails sseDetails;

    /**
     * 索引表信息
     */
    private List<IndexMeta> indexMeta = new ArrayList<IndexMeta>();

    /**
     * 建表时间，单位微秒
     */
    private long creationTime;

    /*
     * 内部接口。请勿使用。
     */
    public DescribeTableResponse(Response meta) {
        super(meta);
    }

    /**
     * 返回表的Meta定义。
     *
     * @return 表的Meta。
     */
    public TableMeta getTableMeta() {
        return tableMeta;
    }

    /*
     * 内部接口。请勿使用。
     */
    public void setTableMeta(TableMeta tableMeta) {
        this.tableMeta = tableMeta;
    }

    /**
     * 返回表的预留吞吐量的更改信息。
     *
     * @return 表的预留吞吐量的更改信息。
     */
    public ReservedThroughputDetails getReservedThroughputDetails() {
        return reservedThroughputDetails;
    }

    /**
     * 所有shard之间的分裂点。
     * 假设某表有两列pk，三个shard。则返回两个一列的分裂点，分别为A和B。
     * 三个shard的range分别为(-inf,-inf) to (A,-inf), (A,-inf) to (B,-inf), (B,-inf) to (+inf,+inf)
     */
    public List<PrimaryKey> getShardSplits() {
        return shardSplits;
    }

    public void setShardSplits(List<PrimaryKey> splits) {
        shardSplits = splits;
    }

    /*
     * 内部接口。请勿使用。
     */
    public void setReservedThroughputDetails(ReservedThroughputDetails reservedThroughputDetails) {
        this.reservedThroughputDetails = reservedThroughputDetails;
    }

    /**
     * 获取表的配置参数。
     *
     * @return 表的配置参数。
     */
    public TableOptions getTableOptions() {
        return tableOptions;
    }

    /**
     * 获取表的Stream信息。
     *
     * @return 表的Stream信息。
     */
    public StreamDetails getStreamDetails() {
        return streamDetails;
    }

    public void setStreamDetails(StreamDetails streamDetails) {
        this.streamDetails = streamDetails;
    }

    /**
     * 获取表的服务器端加密信息。
     *
     * @return 表的服务器端加密信息。
     */
    public SSEDetails getSseDetails() {
        return sseDetails;
    }

    public void setSseDetails(SSEDetails sseDetails) {
        this.sseDetails = sseDetails;
    }

    /**
     * 获取索引表的信息
     *
     * @return 索引表的信息
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
     * 获取建表时间，单位微秒
     *
     * @return long 建表时间
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
