package com.alicloud.openservices.tablestore.model.search.highlight;

import java.util.ArrayList;
import java.util.List;

/**
 * 一行数据中，每个需高亮的Field字段，对应一个{@link HighlightField}
 */
public class HighlightField {
    /**
     * 高亮分片的具体内容
     */
    private List<String> fragments = new ArrayList<String>();


    public List<String> getFragments() {
        return fragments;
    }

    public void setFragments(List<String> fragments) {
        this.fragments = fragments;
    }
}
