package com.alicloud.openservices.tablestore.model.search.sort;

import java.util.List;

/**
 * 排序。可以在SearchQuery中进行设置该项
 */
public class Sort {

    public interface Sorter {
    }

    private List<Sorter> sorters;

    /**
     * 是否允许默认主键排序；
     * <p> 索引创建阶段：
     * <p>  暂不支持显示指定该参数；默认情况下，不包含Nested类型字段索引，会自动添加PrimaryKeySort
     * <p> 查询阶段：
     * <p>  1. {@link com.alicloud.openservices.tablestore.model.search.SearchQuery}、{@link com.alicloud.openservices.tablestore.model.search.agg.TopRowsAggregation},
     * 当指定非PrimaryKeySort的sorter时，默认情况下会主动添加PrimaryKeySort，通过该参数可禁止主动添加PrimaryKeySort
     * <p>  2. {@link com.alicloud.openservices.tablestore.core.protocol.Search.InnerHits} 该参数不生效
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
