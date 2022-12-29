package com.alicloud.openservices.tablestore.model.search;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilder;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupBy;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByBuilder;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.QueryBuilder;
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

    /**
     * 是否返回匹配到的总行数
     */
    private boolean getTotalCount = false;

    private List<Aggregation> aggregationList;

    private List<GroupBy> groupByList;

    private byte[] token;

    public static Builder newBuilder() {
        return new Builder();
    }

    public List<Aggregation> getAggregationList() {
        return aggregationList;
    }


    /**
     * 设置统计聚合中的agg参数。
     * @param aggregationList 使用{@link com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilders}进行构建
     */
    public void setAggregationList(
        List<Aggregation> aggregationList) {
        this.aggregationList = aggregationList;
    }

    public List<GroupBy> getGroupByList() {
        return groupByList;
    }

    /**
     * 设置统计聚合中的groupby参数。
     * @param groupByList 使用{@link com.alicloud.openservices.tablestore.model.search.groupby.GroupByBuilders}进行构建
     */
    public void setGroupByList(List<GroupBy> groupByList) {
        this.groupByList = groupByList;
    }

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
        // Token中编码了Sort条件，所以设置Token时不需要设置Sort
        if (null != token) {
            this.sort = null;
        }
    }

    public SearchQuery() {
    }

    public SearchQuery toCopy() {
        SearchQuery copy = new SearchQuery();
        copy.setAggregationList(this.getAggregationList());
        copy.setCollapse(this.getCollapse());
        copy.setGetTotalCount(this.isGetTotalCount());
        copy.setGroupByList(this.getGroupByList());
        copy.setLimit(this.getLimit());
        copy.setOffset(this.getOffset());
        copy.setQuery(this.getQuery());
        copy.setSort(this.getSort());
        copy.setToken(this.getToken());
        return copy;
    }

    private SearchQuery(Builder builder) {
        setOffset(builder.offset);
        setLimit(builder.limit);
        setQuery(builder.query);
        setCollapse(builder.collapse);
        setSort(builder.sort);
        setGetTotalCount(builder.getTotalCount);
        setAggregationList(builder.aggregationList);
        setGroupByList(builder.groupByList);
        setToken(builder.token);
    }

    public static final class Builder {
        private Integer offset;
        private Integer limit;
        private Query query;
        private Collapse collapse;
        private Sort sort;
        private boolean getTotalCount = false;
        private List<Aggregation> aggregationList;
        private List<GroupBy> groupByList;
        private byte[] token;

        private Builder() {}

        public Builder offset(int val) {
            offset = val;
            return this;
        }

        public Builder limit(int val) {
            limit = val;
            return this;
        }

        public Builder query(QueryBuilder queryBuilder) {
            query = queryBuilder.build();
            return this;
        }

        public Builder query(Query query) {
            this.query = query;
            return this;
        }

        /**
         * 字段折叠
         * 能够实现某个字段的结果去重。
         */
        public Builder collapse(String fieldName) {
            collapse = new Collapse(fieldName);
            return this;
        }

        public Builder sort(Sort val) {
            sort = val;
            return this;
        }

        /**
         * 是否返回匹配到的总行数
         */
        public Builder getTotalCount(boolean val) {
            getTotalCount = val;
            return this;
        }

        /**
         * 添加一个统计聚合中的Agg
         * @param aggregationBuilder 使用{@link com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilders}进行构建
         */
        public Builder addAggregation(AggregationBuilder aggregationBuilder) {
            if (aggregationList == null) {
                aggregationList = new ArrayList<Aggregation>();
            }
            aggregationList.add(aggregationBuilder.build());
            return this;
        }

        /**
         * 添加一个统计聚合中的Agg
         * @param aggregation 使用{@link com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilders}进行构建
         */
        public Builder addAggregation(Aggregation aggregation) {
            if (aggregationList == null) {
                aggregationList = new ArrayList<Aggregation>();
            }
            aggregationList.add(aggregation);
            return this;
        }

        /**
         * 添加一个统计聚合中的GroupBy
         * @param groupByBuilder 使用{@link com.alicloud.openservices.tablestore.model.search.groupby.GroupByBuilders}进行构建
         */
        public Builder addGroupBy(GroupByBuilder groupByBuilder) {
            if (groupByList == null) {
                groupByList = new ArrayList<GroupBy>();
            }
            groupByList.add(groupByBuilder.build());
            return this;
        }

        /**
         * 添加一个统计聚合中的GroupBy
         * @param groupBy 使用{@link com.alicloud.openservices.tablestore.model.search.groupby.GroupByBuilders}进行构建
         */
        public Builder addGroupBy(GroupBy groupBy) {
            if (groupByList == null) {
                groupByList = new ArrayList<GroupBy>();
            }
            groupByList.add(groupBy);
            return this;
        }

        /**
         * 进行翻页的参数
         */
        public Builder token(byte[] val) {
            token = val;
            return this;
        }

        public SearchQuery build() {
            return new SearchQuery(this);
        }
    }
}
