package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.List;

public class GroupByCompositeResult implements GroupByResult {
    private String groupByName;

    private List<GroupByCompositeResultItem> groupByCompositeResultItems;

    private List<String> sourceNames;

    private String nextToken;

    public GroupByCompositeResult() {
    }

    @Override
    public String getGroupByName() {
        return groupByName;
    }

    @Override
    public GroupByType getGroupByType() {
        return GroupByType.GROUP_BY_COMPOSITE;
    }

    public void setGroupByName(String groupByName) {
        this.groupByName = groupByName;
    }

    public List<GroupByCompositeResultItem> getGroupByCompositeResultItems() {
        return groupByCompositeResultItems;
    }

    public void setGroupByCompositeResultItems(List<GroupByCompositeResultItem> groupByCompositeResultItems) {
        this.groupByCompositeResultItems = groupByCompositeResultItems;
    }

    public List<String> getSourceNames() {
        return sourceNames;
    }

    public void setSourceNames(List<String> sourceNames) {
        this.sourceNames = sourceNames;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }
}
