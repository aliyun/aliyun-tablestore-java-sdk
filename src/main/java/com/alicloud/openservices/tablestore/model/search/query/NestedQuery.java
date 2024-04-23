package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 嵌套查询可以查询嵌套的对象/文档。
 * <p>举例：我们的文档是这样的：{"id":"1","os":{"name":"win7","ip":"127.0.0.1"}}，我们想搜索os的name，
 * 但是不能直接查询，需要通过{@link NestedQuery}来进行查询。在"path"设置为“os”，然后query中放一个正常的Query</p>
 */
public class NestedQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_NestedQuery;

    /**
     * 嵌套文档的路径
     */
    private String path;
    /**
     * 一个query
     */
    private Query query;
    /**
     * 多值字段获取文档得分的模式
     */
    private ScoreMode scoreMode;

    /**
     * 获取嵌套文档子文档信息的相关参数
     */
    private InnerHits innerHits;

    private float weight = 1.0f;

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildNestedQuery(this).toByteString();
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public void setInnerHits(InnerHits innerHits) {
        this.innerHits = innerHits;
    }

    public InnerHits getInnerHits() {
        return this.innerHits;
    }

    public ScoreMode getScoreMode() {
        return scoreMode;
    }

    public void setScoreMode(ScoreMode scoreMode) {
        this.scoreMode = scoreMode;
    }

    public float getWeight() {
        return weight;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }

    protected static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder implements QueryBuilder {
        private String path;
        private Query query;
        private ScoreMode scoreMode;
        private InnerHits innerHits;
        private float weight = 1.0f;

        public Builder weight(float weight) {
            this.weight = weight;
            return this;
        }

        private Builder() {}

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder innerHits(InnerHits innerHits) {
            this.innerHits = innerHits;
            return this;
        }

        public Builder query(QueryBuilder queryBuilder) {
            this.query = queryBuilder.build();
            return this;
        }

        public Builder query(Query query) {
            this.query = query;
            return this;
        }

        public Builder scoreMode(ScoreMode scoreMode) {
            this.scoreMode = scoreMode;
            return this;
        }

        @Override
        public NestedQuery build() {
            NestedQuery nestedQuery = new NestedQuery();
            nestedQuery.setPath(this.path);
            nestedQuery.setQuery(this.query);
            nestedQuery.setWeight(this.weight);
            nestedQuery.setScoreMode(this.scoreMode);
            nestedQuery.setInnerHits(this.innerHits);
            return nestedQuery;
        }
    }
}
