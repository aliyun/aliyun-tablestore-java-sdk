package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Nested queries can be used to query nested objects/documents.
 * <p>Example: If our document looks like this: {"id":"1","os":{"name":"win7","ip":"127.0.0.1"}}, and we want to search for the name within "os",
 * we cannot query it directly. Instead, we need to use {@link NestedQuery} for the query. Set the "path" to "os", and then place a regular Query inside the query parameter.</p>
 */
public class NestedQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_NestedQuery;

    /**
     * Path of the nested document
     */
    private String path;
    /**
     * A query
     */
    private Query query;
    /**
     * The mode for obtaining document scores from multi-value fields
     */
    private ScoreMode scoreMode;

    /**
     * Get the relevant parameters for sub-document information of nested documents
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
