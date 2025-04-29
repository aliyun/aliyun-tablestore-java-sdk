package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Find the data where the latitude and longitude fall within the specified rectangle.
 * <p>Use case example: In the scenario of order area profile analysis, if you want to analyze the purchasing power of Area A, which happens to be rectangular, we can achieve this by counting the number of orders (or total price) in Area A.</p>
 * <p>Method: Construct a {@link BoolQuery} in the SearchQuery, place a rectangular geographical location using a {@link GeoBoundingBoxQuery} in its mustQueries, then add another query for counting the orders into the mustQueries, and you will get the desired result.</p>
 */
public class GeoBoundingBoxQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_GeoBoundingBoxQuery;

    private String fieldName;
    /**
     * The latitude and longitude of the top-left corner of the rectangle.
     * <p>Example: "46.24123424, 23.2342424"</p>
     */
    private String topLeft;
    /**
     * The latitude and longitude of the bottom-right corner of the rectangle.
     * <p>Example: "46.24123424, 23.2342424"</p>
     */
    private String bottomRight;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getTopLeft() {
        return topLeft;
    }

    public void setTopLeft(String topLeft) {
        this.topLeft = topLeft;
    }

    public String getBottomRight() {
        return bottomRight;
    }

    public void setBottomRight(String bottomRight) {
        this.bottomRight = bottomRight;
    }

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildGeoBoundingBoxQuery(this).toByteString();
    }

    protected static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder implements QueryBuilder {
        private String fieldName;
        private String topLeft;
        private String bottomRight;

        private Builder() {}

        public Builder field(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * @param topLeft The latitude and longitude of the top-left corner of the rectangle, example: "46.24123424, 23.2342424"
         */
        public Builder topLeft(String topLeft) {
            this.topLeft = topLeft;
            return this;
        }

        /**
         * @param bottomRight The latitude and longitude of the bottom-right corner of the rectangle, example: "46.24123424, 23.2342424"
         */
        public Builder bottomRight(String bottomRight) {
            this.bottomRight = bottomRight;
            return this;
        }

        @Override
        public GeoBoundingBoxQuery build() {
            GeoBoundingBoxQuery boundingBoxQuery = new GeoBoundingBoxQuery();
            boundingBoxQuery.setBottomRight(this.bottomRight);
            boundingBoxQuery.setTopLeft(this.topLeft);
            boundingBoxQuery.setFieldName(this.fieldName);
            return boundingBoxQuery;
        }
    }
}
