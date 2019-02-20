package com.alicloud.openservices.tablestore.model.search.sort;

import java.util.List;

/**
 * 排序。可以在SearchQuery中进行设置该项
 */
public class Sort {

    public interface Sorter {
    }

    private List<Sorter> sorters;

    public Sort(List<Sorter> sorters) {
        this.sorters = sorters;
    }

    public List<Sorter> getSorters() {
        return sorters;
    }

    public void setSorters(List<Sorter> sorters) {
        this.sorters = sorters;
    }
}
