package com.alicloud.openservices.tablestore.model.search.highlight;

import java.util.ArrayList;
import java.util.List;

/**
 * In a row of data, each Field that needs to be highlighted corresponds to one {@link HighlightField}.
 */
public class HighlightField {
    /**
     * Highlight the specific content of the shard.
     */
    private List<String> fragments = new ArrayList<String>();


    public List<String> getFragments() {
        return fragments;
    }

    public void setFragments(List<String> fragments) {
        this.fragments = fragments;
    }
}
