package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.ArrayList;
import java.util.List;

public class SplitTimeseriesScanTaskResponse extends Response {

    private List<TimeseriesScanSplitInfo> splitInfos;

    public SplitTimeseriesScanTaskResponse(Response meta) {
        super(meta);
    }

    public List<TimeseriesScanSplitInfo> getSplitInfos() {
        if (splitInfos == null) {
            splitInfos = new ArrayList<TimeseriesScanSplitInfo>();
        }
        return splitInfos;
    }

    public void setSplitInfos(List<TimeseriesScanSplitInfo> splitInfos) {
        this.splitInfos = splitInfos;
    }
}
