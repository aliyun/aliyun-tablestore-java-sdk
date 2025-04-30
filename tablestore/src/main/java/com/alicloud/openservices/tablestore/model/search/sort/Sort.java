package com.alicloud.openservices.tablestore.model.search.sort;

import java.util.List;

/**
 * Sorting. This can be set in the SearchQuery.
 */
public class Sort {

    public interface Sorter {
    }

    private List<Sorter> sorters;

    /**
     * Whether to allow default primary key sorting;
     * <p> Index creation phase:
     * <p> Explicitly specifying this parameter is temporarily unsupported; by default, if the index does not include Nested type fields, PrimaryKeySort will be automatically added.
     * <p> Query phase:
     * <p> 1. {@link com.alicloud.openservices.tablestore.model.search.SearchQuery}, {@link com.alicloud.openservices.tablestore.model.search.agg.TopRowsAggregation},
     * when a sorter other than PrimaryKeySort is specified, PrimaryKeySort will be added by default. This parameter can be used to disable the automatic addition of PrimaryKeySort.
     * <p> 2. {@link com.alicloud.openservices.tablestore.core.protocol.Search.InnerHits} This parameter has no effect.
     */
    private Boolean disableDefaultPkSorter;

    public Sort() {
    }

    public Sort(List<Sorter> sorters) {
        this.sorters = sorters;
    }

    public Sort(List<Sorter> sorters, boolean disableDefaultPkSorter) {
        this.sorters = sorters;
        this.disableDefaultPkSorter = disableDefaultPkSorter;
    }

    public List<Sorter> getSorters() {
        return sorters;
    }

    public void setSorters(List<Sorter> sorters) {
        this.sorters = sorters;
    }

    public void setDisableDefaultPkSorter(boolean disableDefaultPkSorter) {
        this.disableDefaultPkSorter = disableDefaultPkSorter;
    }

    public Boolean getDisableDefaultPkSorter() {
        return this.disableDefaultPkSorter;
    }
}
