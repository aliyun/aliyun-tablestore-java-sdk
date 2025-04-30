package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.List;

public class ListSearchIndexResponse extends Response {

    private List<SearchIndexInfo> indexInfos;

    public ListSearchIndexResponse(Response meta) {
        super(meta);
    }

    public List<SearchIndexInfo> getIndexInfos() {
        return indexInfos;
    }

    public void setIndexInfos(List<SearchIndexInfo> indexInfos) {
        this.indexInfos = indexInfos;
    }


}
