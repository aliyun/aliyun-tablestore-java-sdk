package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.model.Response;

public class DescribeSearchIndexResponse extends Response implements Jsonizable {

    private IndexSchema schema;
    private SyncStat syncStat;
    private MeteringInfo meteringInfo;

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
        sb.append(newline.substring(0, newline.length() - 2));
        sb.append("}");
    }
}