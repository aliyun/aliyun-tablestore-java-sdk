package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.*;

public class SearchIndexSplitsOptionsTest {

    @Test
    public void getIndexName() {
        SearchIndexSplitsOptions searchIndexSplitsOptions = new SearchIndexSplitsOptions("123");
        assertEquals("123", searchIndexSplitsOptions.getIndexName());
    }

    @Test
    public void setIndexName() {
        SearchIndexSplitsOptions searchIndexSplitsOptions = new SearchIndexSplitsOptions();
        searchIndexSplitsOptions.setIndexName("123");
        assertEquals("123", searchIndexSplitsOptions.getIndexName());
    }
}