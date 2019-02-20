package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;

/**
 * 构建SearchQuery，该实体类会通过SearchRequest传递给服务器，告诉服务器我们的搜索参数
 */
public class SearchQuery {

    /**
     * 分页起始数量
     */
    private Integer offset;

    /**
     * 分页大小，即返回的行数
     */
    private Integer limit;

    /**
     * 查询语句
     */
    private Query query;

    /**
     * 字段折叠
     * 能够实现某个字段的结果去重。
     */
    private Collapse collapse;

    /**
     * 排序
     * <p>设置结果的排序方式，该参数支持多字段排序</p>
     */
    private Sort sort;

    private boolean getTotalCount = false;

    private byte[] token;

    public Integer getOffset() {
        return offset;
    }

    public SearchQuery setOffset(Integer offset) {
        this.offset = offset;
        return this;
    }

    public Integer getLimit() {
        return limit;
    }

    public SearchQuery setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Query getQuery() {
        return query;
    }

    public SearchQuery setQuery(Query query) {
        this.query = query;
        return this;
    }

    public Collapse getCollapse() {
        return collapse;
    }

    public SearchQuery setCollapse(Collapse collapse) {
        this.collapse = collapse;
        return this;
    }

    public Sort getSort() {
        return sort;
    }

    public SearchQuery setSort(Sort sort) {
        this.sort = sort;
        return this;
    }

    public boolean isGetTotalCount() {
        return getTotalCount;
    }

    public void setGetTotalCount(boolean getTotalCount) {
        this.getTotalCount = getTotalCount;
    }

    public byte[] getToken() {
        return token;
    }

    public void setToken(byte[] token) {
        this.token = token;
        this.sort = null; // Token中编码了Sort条件，所以设置Token时不需要设置Sort
    }
}
