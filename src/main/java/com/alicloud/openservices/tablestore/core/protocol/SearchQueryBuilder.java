package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.google.protobuf.ByteString;

public class SearchQueryBuilder {

    public static Search.QueryType buildQueryType(QueryType type) {
        switch (type) {
            case QueryType_MatchQuery:
                return Search.QueryType.MATCH_QUERY;
            case QueryType_MatchPhraseQuery:
                return Search.QueryType.MATCH_PHRASE_QUERY;
            case QueryType_TermQuery:
                return Search.QueryType.TERM_QUERY;
            case QueryType_TermsQuery:
                return Search.QueryType.TERMS_QUERY;
            case QueryType_RangeQuery:
                return Search.QueryType.RANGE_QUERY;
            case QueryType_PrefixQuery:
                return Search.QueryType.PREFIX_QUERY;
            case QueryType_BoolQuery:
                return Search.QueryType.BOOL_QUERY;
            case QueryType_ConstScoreQuery:
                return Search.QueryType.CONST_SCORE_QUERY;
            case QueryType_FunctionScoreQuery:
                return Search.QueryType.FUNCTION_SCORE_QUERY;
            case QueryType_NestedQuery:
                return Search.QueryType.NESTED_QUERY;
            case QueryType_WildcardQuery:
                return Search.QueryType.WILDCARD_QUERY;
            case QueryType_MatchAllQuery:
                return Search.QueryType.MATCH_ALL_QUERY;
            case QueryType_GeoBoundingBoxQuery:
                return Search.QueryType.GEO_BOUNDING_BOX_QUERY;
            case QueryType_GeoDistanceQuery:
                return Search.QueryType.GEO_DISTANCE_QUERY;
            case QueryType_GeoPolygonQuery:
                return Search.QueryType.GEO_POLYGON_QUERY;
            default:
                throw new IllegalArgumentException("unknown queryType: " + type.name());
        }
    }

    public static Search.Query buildQuery(Query query) {
        Search.Query.Builder builder = Search.Query.newBuilder();
        builder.setType(buildQueryType(query.getQueryType()));
        builder.setQuery(query.serialize());
        return builder.build();
    }

    public static Search.MatchAllQuery buildMatchAllQuery() {
        Search.MatchAllQuery.Builder builder = Search.MatchAllQuery.newBuilder();
        return builder.build();
    }

    public static Search.MatchQuery buildMatchQuery(MatchQuery query) {
        Search.MatchQuery.Builder builder = Search.MatchQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        builder.setText(query.getText());
        if (query.getMinimumShouldMatch() != null) {
            builder.setMinimumShouldMatch(query.getMinimumShouldMatch());
        }
        if (query.getOperator() != null) {
            switch (query.getOperator()) {
                case OR:
                    builder.setOperator(Search.QueryOperator.OR);
                    break;
                case AND:
                    builder.setOperator(Search.QueryOperator.AND);
                    break;
                default:
                    throw new IllegalArgumentException("unsupported operator: " + query.getOperator());
            }
        }
        return builder.build();
    }

    public static Search.MatchPhraseQuery buildMatchPhraseQuery(MatchPhraseQuery query) {
        Search.MatchPhraseQuery.Builder builder = Search.MatchPhraseQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        builder.setText(query.getText());
        return builder.build();
    }

    public static Search.TermQuery buildTermQuery(TermQuery query) {
        Search.TermQuery.Builder builder = Search.TermQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        builder.setTerm(ByteString.copyFrom(SearchVariantType.toVariant(query.getTerm())));
        return builder.build();
    }

    public static Search.TermsQuery buildTermsQuery(TermsQuery query) {
        Search.TermsQuery.Builder builder = Search.TermsQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        if (query.getTerms() == null) {
            throw new IllegalArgumentException("terms is null");
        }
        for (ColumnValue term : query.getTerms()) {
            builder.addTerms(ByteString.copyFrom(SearchVariantType.toVariant(term)));
        }
        return builder.build();
    }

    public static Search.RangeQuery buildRangeQuery(RangeQuery query) {
        Search.RangeQuery.Builder builder = Search.RangeQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        if (query.getFrom() != null) {
            builder.setRangeFrom(ByteString.copyFrom(SearchVariantType.toVariant(query.getFrom())));
            builder.setIncludeLower(query.isIncludeLower());
        }
        if (query.getTo() != null) {
            builder.setRangeTo(ByteString.copyFrom(SearchVariantType.toVariant(query.getTo())));
            builder.setIncludeUpper(query.isIncludeUpper());
        }
        return builder.build();
    }

    public static Search.PrefixQuery buildPrefixQuery(PrefixQuery query) {
        Search.PrefixQuery.Builder builder = Search.PrefixQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        builder.setPrefix(query.getPrefix());
        return builder.build();
    }

    public static Search.WildcardQuery buildWildcardQuery(WildcardQuery query) {
        Search.WildcardQuery.Builder builder = Search.WildcardQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        builder.setValue(query.getValue());
        return builder.build();
    }

    public static Search.BoolQuery buildBoolQuery(BoolQuery query) {
        Search.BoolQuery.Builder builder = Search.BoolQuery.newBuilder();
        if (query.getMinimumShouldMatch() != null) {
            builder.setMinimumShouldMatch(query.getMinimumShouldMatch());
        }
        if (query.getMustQueries() != null) {
            for (Query q : query.getMustQueries()) {
                builder.addMustQueries(buildQuery(q));
            }
        }
        if (query.getMustNotQueries() != null) {
            for (Query q : query.getMustNotQueries()) {
                builder.addMustNotQueries(buildQuery(q));
            }
        }
        if (query.getShouldQueries() != null) {
            for (Query q : query.getShouldQueries()) {
                builder.addShouldQueries(buildQuery(q));
            }
        }
        if (query.getFilterQueries() != null) {
            for (Query q : query.getFilterQueries()) {
                builder.addFilterQueries(buildQuery(q));
            }
        }
        return builder.build();
    }

    public static Search.ConstScoreQuery buildConstScoreQuery(ConstScoreQuery query) {
        Search.ConstScoreQuery.Builder builder = Search.ConstScoreQuery.newBuilder();
        builder.setFilter(SearchQueryBuilder.buildQuery(query.getFilter()));
        return builder.build();
    }

    public static Search.FieldValueFactor buildFieldValueFactor(FieldValueFactor fieldValueFactor) {
        Search.FieldValueFactor.Builder builder = Search.FieldValueFactor.newBuilder();
        builder.setFieldName(fieldValueFactor.getFieldName());
        return builder.build();
    }

    public static Search.FunctionScoreQuery buildFunctionScoreQuery(FunctionScoreQuery query) {
        Search.FunctionScoreQuery.Builder builder = Search.FunctionScoreQuery.newBuilder();
        builder.setQuery(SearchQueryBuilder.buildQuery(query.getQuery()));
        builder.setFieldValueFactor(buildFieldValueFactor(query.getFieldValueFactor()));
        return builder.build();
    }

    public static Search.ScoreMode buildScoreMode(ScoreMode scoreMode) {
        switch (scoreMode) {
            case Max:
                return Search.ScoreMode.SCORE_MODE_MAX;
            case Min:
                return Search.ScoreMode.SCORE_MODE_MIN;
            case Avg:
                return Search.ScoreMode.SCORE_MODE_AVG;
            case Total:
                return Search.ScoreMode.SCORE_MODE_TOTAL;
            case None:
                return Search.ScoreMode.SCORE_MODE_NONE;
            default:
                throw new IllegalArgumentException("unknown scoreMode: " + scoreMode.name());
        }
    }

    public static Search.NestedQuery buildNestedQuery(NestedQuery query) {
        Search.NestedQuery.Builder builder = Search.NestedQuery.newBuilder();
        builder.setQuery(SearchQueryBuilder.buildQuery(query.getQuery()));
        builder.setPath(query.getPath());
        builder.setScoreMode(buildScoreMode(query.getScoreMode()));
        return builder.build();
    }

    public static Search.GeoBoundingBoxQuery buildGeoBoundingBoxQuery(GeoBoundingBoxQuery query) {
        Search.GeoBoundingBoxQuery.Builder builder = Search.GeoBoundingBoxQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        builder.setTopLeft(query.getTopLeft());
        builder.setBottomRight(query.getBottomRight());
        return builder.build();
    }

    public static Search.GeoDistanceQuery buildGeoDistanceQuery(GeoDistanceQuery query) {
        Search.GeoDistanceQuery.Builder builder = Search.GeoDistanceQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        builder.setCenterPoint(query.getCenterPoint());
        builder.setDistance(query.getDistanceInMeter());
        return builder.build();
    }

    public static Search.GeoPolygonQuery buildGeoPolygonQuery(GeoPolygonQuery query) {
        Search.GeoPolygonQuery.Builder builder = Search.GeoPolygonQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        builder.addAllPoints(query.getPoints());
        return builder.build();
    }
}
