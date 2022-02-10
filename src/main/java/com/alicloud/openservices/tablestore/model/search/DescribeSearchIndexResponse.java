package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.model.Response;

import java.util.ArrayList;
import java.util.List;

public class DescribeSearchIndexResponse extends Response implements Jsonizable {

    private IndexSchema schema;
    private SyncStat syncStat;
    private MeteringInfo meteringInfo;

    private String brotherIndexName;
    private List<QueryFlowWeight> queryFlowWeight;
    private Long createTime;

    /**
     * <p>索引数据的TTL时间，单位为秒。</p>
     */
    private Integer timeToLive;

    public DescribeSearchIndexResponse(Response meta) {
        super(meta);
    }

    public IndexSchema getSchema() {
        return schema;
    }

    public void setSchema(IndexSchema schema) {
        this.schema = schema;
    }

    public SyncStat getSyncStat() {
        return syncStat;
    }

    public void setSyncStat(SyncStat syncStat) {
        this.syncStat = syncStat;
    }

    public MeteringInfo getMeteringInfo() {
        return meteringInfo;
    }

    public void setMeteringInfo(MeteringInfo meteringInfo) {
        this.meteringInfo = meteringInfo;
    }

    public void setBrotherIndexName(String brotherIndexName) {
        this.brotherIndexName = brotherIndexName;
    }

    public String getBrotherIndexName() {
        return brotherIndexName;
    }

    public void setQueryFlowWeight(List<QueryFlowWeight> queryFlowWeight) {
        this.queryFlowWeight = queryFlowWeight;
    }

    public void addQueryFlowWeight(QueryFlowWeight w) {
        if (this.queryFlowWeight == null) {
            this.queryFlowWeight = new ArrayList<QueryFlowWeight>();
        }
        this.queryFlowWeight.add(w);
    }

    public List<QueryFlowWeight> getQueryFlowWeight() {
        return queryFlowWeight;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public Integer getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(Integer timeToLive) {
        this.timeToLive = timeToLive;
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
        sb.append("\"IndexSchema\": ");
        if (schema != null) {
            schema.jsonize(sb, newline + "  ");
        } else {
            sb.append("null");
        }
        sb.append(",");
        sb.append(newline);
        sb.append("\"SyncStat\": ");
        if (syncStat != null) {
            syncStat.jsonize(sb, newline + "  ");
        } else {
            sb.append("null");
        }

        if (brotherIndexName != null) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"BrotherIndexName\": \"");
            sb.append(brotherIndexName);
            sb.append("\"");
        }

        if (queryFlowWeight != null && queryFlowWeight.size() > 0) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"QueryFlowWeight\": [");
            boolean first = true;
            for (QueryFlowWeight queryFlowWeight : this.queryFlowWeight) {
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                    sb.append(newline + " ");
                }
                queryFlowWeight.jsonize(sb, newline + " ");
            }
            sb.append("]");
        }

        if (createTime != null) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"CreateTime\": ");
            sb.append(createTime);
        }
        if (timeToLive != null) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"TimeToLive\": ");
            sb.append(timeToLive);
        }

        sb.append(newline.substring(0, newline.length() - 2));
        sb.append("}");
    }
}