package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.core.protocol.SearchGroupByBuilder;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilder;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

public class GroupByComposite implements GroupBy {
    private final GroupByType groupByType = GroupByType.GROUP_BY_COMPOSITE;

    /**
     * GroupByComposite的名字
     */
    private String groupByName;

    /**
     * 支持对多种类型的多列进行分组统计
     * <p>目前支持:</p>
     * <li>{@link GroupByField}</li>
     * <li>{@link GroupByHistogram}</li>
     * <li>{@link GroupByDateHistogram}</li>
     */
    private List<GroupBy> sources;

    /**
     * GroupByComposite结果内会返回nextToken，用于支持分组翻页
     */
    private String nextToken;

    /**
     * 返回分组的数量
     */
    private Integer size;

    /**
     * 返回分组的数量；软限制，允许设置大于服务端最大限制值。当该值超过服务端最大值限制后被修正为最大值。
     * <p> 实际值返回分组结果数量为：min(suggestedSize, 服务端分组数量限制，总分组数量)
     * <p> 适用场景：重吞吐的场景，一般是对接计算系统，比如spark、presto等。
     */
    private Integer suggestedSize;

    /**
     * 子groupBy
     */
    private List<GroupBy> subGroupBys;

    /**
     * 子聚合
     */
    private List<Aggregation> subAggregations;

    public GroupByComposite() {
    }

    @Override
    public String getGroupByName() {
        return this.groupByName;
    }

    @Override
    public GroupByType getGroupByType() {
        return groupByType;
    }

    @Override
    public ByteString serialize() {
        return SearchGroupByBuilder.buildGroupByComposite(this).toByteString();
    }

    public void setGroupByName(String groupByName) {
        this.groupByName = groupByName;
    }

    public void setSources(List<GroupBy> sources) {
        this.sources = sources;
    }

    public List<GroupBy> getSources() {
        return this.sources;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }

    public String getNextToken() {
        return this.nextToken;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Integer getSize() {
        return this.size;
    }

    public Integer getSuggestedSize() {
        return suggestedSize;
    }

    public void setSuggestedSize(int suggestedSize) {
        this.suggestedSize = suggestedSize;
    }

    public void setSubGroupBys(List<GroupBy> subGroupBys) {
        this.subGroupBys = subGroupBys;
    }

    public List<GroupBy> getSubGroupBys() {
        return this.subGroupBys;
    }

    public void setSubAggregations(List<Aggregation> subAggregations) {
        this.subAggregations = subAggregations;
    }

    public List<Aggregation> getSubAggregations() {
        return this.subAggregations;
    }

    public GroupByComposite(GroupByComposite.Builder builder) {
        groupByName = builder.groupByName;
        sources = builder.sources;
        nextToken = builder.nextToken;
        size = builder.size;
        suggestedSize = builder.suggestedSize;
        subGroupBys = builder.subGroupBys;
        subAggregations = builder.subAggregations;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements GroupByBuilder {
        private String groupByName;

        private List<GroupBy> sources;

        private String nextToken;

        private Integer size;
        private Integer suggestedSize;

        private List<GroupBy> subGroupBys;

        private List<Aggregation> subAggregations;

        public Builder() {}

        public Builder groupByName(String groupByName) {
            this.groupByName = groupByName;
            return this;
        }

        public Builder addSources(GroupBy sourceGroupBy) {
            if (sources == null) {
                this.sources = new ArrayList<GroupBy>();
            }

            this.sources.add(sourceGroupBy);
            return this;
        }

        public Builder addSources(GroupByBuilder sourceGroupByBuilder) {
            return addSources(sourceGroupByBuilder.build());
        }

        public Builder setSources(List<GroupBy> sources) {
            this.sources = sources;
            return this;
        }

        public Builder nextToken(String nextToken) {
            this.nextToken = nextToken;
            return this;
        }

        public Builder size(int size) {
            this.size = size;
            return this;
        }

        public Builder suggestedSize(int suggestedSize) {
            this.suggestedSize = suggestedSize;
            return this;
        }

        public Builder addSubAggregation(AggregationBuilder aggregationBuilder) {
            if (subAggregations == null) {
                subAggregations = new ArrayList<Aggregation>();
            }
            this.subAggregations.add(aggregationBuilder.build());
            return this;
        }

        public Builder addSubAggregation(Aggregation aggregation) {
            if (subAggregations == null) {
                subAggregations = new ArrayList<Aggregation>();
            }
            this.subAggregations.add(aggregation);
            return this;
        }

        public Builder addSubGroupBy(GroupByBuilder groupByBuilder) {
            if (subGroupBys == null) {
                subGroupBys = new ArrayList<GroupBy>();
            }
            this.subGroupBys.add(groupByBuilder.build());
            return this;
        }

        public Builder addSubGroupBy(GroupBy groupBy) {
            if (subGroupBys == null) {
                subGroupBys = new ArrayList<GroupBy>();
            }
            this.subGroupBys.add(groupBy);
            return this;
        }

        @Override
        public GroupByComposite build() {
            return new GroupByComposite(this);
        }
    }
}
