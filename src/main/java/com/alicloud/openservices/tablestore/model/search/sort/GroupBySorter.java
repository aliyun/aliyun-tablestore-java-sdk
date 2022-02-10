package com.alicloud.openservices.tablestore.model.search.sort;

public class GroupBySorter {

    private GroupKeySort groupKeySort;
    private RowCountSort rowCountSort;
    private SubAggSort subAggSort;

    private GroupBySorter(GroupKeySort groupKeySort, RowCountSort rowCountSort, SubAggSort subAggSort) {
        this.groupKeySort = groupKeySort;
        this.rowCountSort = rowCountSort;
        this.subAggSort = subAggSort;
    }

    public GroupBySorter() {
    }

    public GroupKeySort getGroupKeySort() {
        return groupKeySort;
    }

    public RowCountSort getRowCountSort() {
        return rowCountSort;
    }

    public SubAggSort getSubAggSort() {
        return subAggSort;
    }

    public GroupBySorter setGroupKeySort(GroupKeySort groupKeySort) {
        this.groupKeySort = groupKeySort;
        return this;
    }

    public GroupBySorter setRowCountSort(RowCountSort rowCountSort) {
        this.rowCountSort = rowCountSort;
        return this;
    }

    public GroupBySorter setSubAggSort(SubAggSort subAggSort) {
        this.subAggSort = subAggSort;
        return this;
    }

    public static GroupBySorter groupKeySortInAsc() {
        return new GroupBySorter(GroupKeySort.asc(), null, null);
    }

    public static GroupBySorter groupKeySortInDesc() {
        return new GroupBySorter(GroupKeySort.desc(), null, null);
    }

    public static GroupBySorter rowCountSortInAsc() {
        return new GroupBySorter(null, RowCountSort.asc(), null);
    }

    public static GroupBySorter rowCountSortInDesc() {
        return new GroupBySorter(null, RowCountSort.desc(), null);
    }

    public static GroupBySorter subAggSortInAsc(String subAggName) {
        return new GroupBySorter(null, null, SubAggSort.asc(subAggName));
    }

    public static GroupBySorter subAggSortInDesc(String subAggName) {
        return new GroupBySorter(null, null, SubAggSort.desc(subAggName));
    }

}



