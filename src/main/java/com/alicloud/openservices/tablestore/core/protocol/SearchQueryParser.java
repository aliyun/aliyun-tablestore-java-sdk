package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.ConstScoreQuery;
import com.alicloud.openservices.tablestore.model.search.query.ExistsQuery;
import com.alicloud.openservices.tablestore.model.search.query.FieldValueFactor;
import com.alicloud.openservices.tablestore.model.search.query.FunctionScoreQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoBoundingBoxQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoDistanceQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoPolygonQuery;
import com.alicloud.openservices.tablestore.model.search.query.KnnVectorQuery;
import com.alicloud.openservices.tablestore.model.search.query.MatchAllQuery;
import com.alicloud.openservices.tablestore.model.search.query.MatchPhraseQuery;
import com.alicloud.openservices.tablestore.model.search.query.MatchQuery;
import com.alicloud.openservices.tablestore.model.search.query.NestedQuery;
import com.alicloud.openservices.tablestore.model.search.query.PrefixQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.QueryOperator;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;
import com.alicloud.openservices.tablestore.model.search.query.ScoreMode;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermsQuery;
import com.alicloud.openservices.tablestore.model.search.query.WildcardQuery;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.alicloud.openservices.tablestore.core.protocol.SearchInnerHitsParser.toInnerHits;


/**
 * {@link Query} deserialization tool class. For serialization, please refer to {@link SearchQueryBuilder}
 */
public class SearchQueryParser {

    public static Query toQuery(byte[] bytes) throws IOException {
        Search.Query query = Search.Query.parseFrom(bytes);
        return toQuery(query);
    }

    static Query toQuery(Search.Query query) throws IOException {
        Preconditions.checkArgument(query.hasType(), "Search.Query must has type");
        Preconditions.checkArgument(query.hasQuery(), "Search.Query must has query");
        ByteString queryByteString = query.getQuery();
        switch (query.getType()) {
            case MATCH_QUERY:
                return toMatchQuery(queryByteString);
            case MATCH_PHRASE_QUERY:
                return toMatchPhraseQuery(queryByteString);
            case TERM_QUERY:
                return toTermQuery(queryByteString);
            case TERMS_QUERY:
                return toTermsQuery(queryByteString);
            case RANGE_QUERY:
                return toRangeQuery(queryByteString);
            case PREFIX_QUERY:
                return toPrefixQuery(queryByteString);
            case BOOL_QUERY:
                return toBoolQuery(queryByteString);
            case CONST_SCORE_QUERY:
                return toConstScoreQuery(queryByteString);
            case FUNCTION_SCORE_QUERY:
                return toFunctionScoreQuery(queryByteString);
            case NESTED_QUERY:
                return toNestedQuery(queryByteString);
            case WILDCARD_QUERY:
                return toWildcardQuery(queryByteString);
            case MATCH_ALL_QUERY:
                return toMatchAllQuery();
            case GEO_BOUNDING_BOX_QUERY:
                return toGeoBoundingBoxQuery(queryByteString);
            case GEO_DISTANCE_QUERY:
                return toGeoDistanceQuery(queryByteString);
            case GEO_POLYGON_QUERY:
                return toGeoPolygonQuery(queryByteString);
            case EXISTS_QUERY:
                return toExistsQuery(queryByteString);
            case KNN_VECTOR_QUERY:
                return toKnnVectorQuery(queryByteString);
            default:
                throw new IllegalArgumentException("unknown queryType: " + query.getType().name());
        }
    }

    private static List<Query> toQueryList(List<Search.Query> pbQueryList) throws IOException {
        List<Query> queryList = new ArrayList<Query>();
        for (Search.Query pb : pbQueryList) {
            queryList.add(toQuery(pb));
        }
        return queryList;
    }

    private static MatchAllQuery toMatchAllQuery() {
        return new MatchAllQuery();
    }

    private static MatchQuery toMatchQuery(ByteString queryByteString) throws IOException {
        Search.MatchQuery pb = Search.MatchQuery.parseFrom(queryByteString);
        MatchQuery query = new MatchQuery();
        if (pb.hasFieldName()) {
            query.setFieldName(pb.getFieldName());
        }
        if (pb.hasWeight()) {
            query.setWeight(pb.getWeight());
        }
        if (pb.hasText()) {
            query.setText(pb.getText());
        }
        if (pb.hasMinimumShouldMatch()) {
            query.setMinimumShouldMatch(pb.getMinimumShouldMatch());
        }
        if (pb.hasOperator()) {
            Search.QueryOperator operator = pb.getOperator();
            switch (operator) {
                case OR:
                    query.setOperator(QueryOperator.OR);
                    break;
                case AND:
                    query.setOperator(QueryOperator.AND);
                    break;
                default:
                    throw new IllegalArgumentException("unsupported operator: " + operator.name());
            }
        }
        return query;
    }

    private static MatchPhraseQuery toMatchPhraseQuery(ByteString queryByteString) throws IOException {
        Search.MatchPhraseQuery pb = Search.MatchPhraseQuery.parseFrom(queryByteString);
        MatchPhraseQuery query = new MatchPhraseQuery();
        if (pb.hasFieldName()) {
            query.setFieldName(pb.getFieldName());
        }
        if (pb.hasWeight()) {
            query.setWeight(pb.getWeight());
        }
        if (pb.hasText()) {
            query.setText(pb.getText());
        }
        return query;
    }

    private static TermQuery toTermQuery(ByteString queryByteString) throws IOException {
        Search.TermQuery pb = Search.TermQuery.parseFrom(queryByteString);
        TermQuery query = new TermQuery();
        if (pb.hasFieldName()) {
            query.setFieldName(pb.getFieldName());
        }
        if (pb.hasWeight()) {
            query.setWeight(pb.getWeight());
        }
        if (pb.hasTerm()) {
            ColumnValue columnValue = ValueUtil.toColumnValue(SearchVariantType.getValue(pb.getTerm().toByteArray()));
            query.setTerm(columnValue);
        }
        return query;
    }

    private static TermsQuery toTermsQuery(ByteString queryByteString) throws IOException {
        Search.TermsQuery pb = Search.TermsQuery.parseFrom(queryByteString);
        TermsQuery query = new TermsQuery();
        if (pb.hasFieldName()) {
            query.setFieldName(pb.getFieldName());
        }
        if (pb.hasWeight()) {
            query.setWeight(pb.getWeight());
        }
        List<ColumnValue> terms = new ArrayList<ColumnValue>();
        for (ByteString byteString : pb.getTermsList()) {
            ColumnValue columnValue = ValueUtil.toColumnValue(SearchVariantType.getValue(byteString.toByteArray()));
            terms.add(columnValue);
        }
        query.setTerms(terms);
        return query;
    }

    private static RangeQuery toRangeQuery(ByteString queryByteString) throws IOException {
        Search.RangeQuery pb = Search.RangeQuery.parseFrom(queryByteString);
        RangeQuery query = new RangeQuery();
        if (pb.hasFieldName()) {
            query.setFieldName(pb.getFieldName());
        }
        if (pb.hasRangeTo()) {
            ColumnValue columnValue = ValueUtil.toColumnValue(SearchVariantType.getValue(pb.getRangeTo().toByteArray()));
            query.setTo(columnValue);
        }
        if (pb.hasRangeFrom()) {
            ColumnValue columnValue = ValueUtil.toColumnValue(SearchVariantType.getValue(pb.getRangeFrom().toByteArray()));
            query.setFrom(columnValue);
        }
        if (pb.hasIncludeLower()) {
            query.setIncludeLower(pb.getIncludeLower());
        }
        if (pb.hasIncludeUpper()) {
            query.setIncludeUpper(pb.getIncludeUpper());
        }
        return query;
    }

    private static PrefixQuery toPrefixQuery(ByteString queryByteString) throws IOException {
        Search.PrefixQuery pb = Search.PrefixQuery.parseFrom(queryByteString);
        PrefixQuery query = new PrefixQuery();
        if (pb.hasFieldName()) {
            query.setFieldName(pb.getFieldName());
        }
        if (pb.hasWeight()) {
            query.setWeight(pb.getWeight());
        }
        if (pb.hasPrefix()) {
            query.setPrefix(pb.getPrefix());
        }
        return query;
    }

    private static WildcardQuery toWildcardQuery(ByteString queryByteString) throws IOException {
        Search.WildcardQuery pb = Search.WildcardQuery.parseFrom(queryByteString);
        WildcardQuery query = new WildcardQuery();
        if (pb.hasFieldName()) {
            query.setFieldName(pb.getFieldName());
        }
        if (pb.hasWeight()) {
            query.setWeight(pb.getWeight());
        }
        if (pb.hasValue()) {
            query.setValue(pb.getValue());
        }
        return query;
    }

    private static BoolQuery toBoolQuery(ByteString queryByteString) throws IOException {
        Search.BoolQuery pb = Search.BoolQuery.parseFrom(queryByteString);
        BoolQuery query = new BoolQuery();
        if (pb.hasMinimumShouldMatch()) {
            query.setMinimumShouldMatch(pb.getMinimumShouldMatch());
        }
        query.setMustQueries(toQueryList(pb.getMustQueriesList()));
        query.setMustNotQueries(toQueryList(pb.getMustNotQueriesList()));
        query.setShouldQueries(toQueryList(pb.getShouldQueriesList()));
        query.setFilterQueries(toQueryList(pb.getFilterQueriesList()));
        return query;
    }

    private static ConstScoreQuery toConstScoreQuery(ByteString queryByteString) throws IOException {
        Search.ConstScoreQuery pb = Search.ConstScoreQuery.parseFrom(queryByteString);
        ConstScoreQuery query = new ConstScoreQuery();
        if (pb.hasFilter()) {
            query.setFilter(toQuery(pb.getFilter()));
        }
        return query;
    }

    private static FieldValueFactor toFieldValueFactor(Search.FieldValueFactor pb) {
        Preconditions.checkArgument(pb.hasFieldName(), "FieldValueFactor must has fieldName");
        return new FieldValueFactor(pb.getFieldName());
    }

    private static FunctionScoreQuery toFunctionScoreQuery(ByteString queryByteString) throws IOException {
        Search.FunctionScoreQuery pb = Search.FunctionScoreQuery.parseFrom(queryByteString);
        Preconditions.checkArgument(pb.hasFieldValueFactor(), "FunctionScoreQuery must has FieldValueFactor");
        Preconditions.checkArgument(pb.hasQuery(), "FunctionScoreQuery must has Query");
        return new FunctionScoreQuery(toQuery(pb.getQuery()), toFieldValueFactor(pb.getFieldValueFactor()));
    }

    private static ScoreMode toScoreMode(Search.ScoreMode scoreMode) {
        switch (scoreMode) {
            case SCORE_MODE_MAX:
                return ScoreMode.Max;
            case SCORE_MODE_MIN:
                return ScoreMode.Min;
            case SCORE_MODE_AVG:
                return ScoreMode.Avg;
            case SCORE_MODE_TOTAL:
                return ScoreMode.Total;
            case SCORE_MODE_NONE:
                return ScoreMode.None;
            default:
                throw new IllegalArgumentException("unknown scoreMode: " + scoreMode.name());
        }
    }

    private static NestedQuery toNestedQuery(ByteString queryByteString) throws IOException {
        Search.NestedQuery pb = Search.NestedQuery.parseFrom(queryByteString);
        NestedQuery query = new NestedQuery();
        if (pb.hasPath()) {
            query.setPath(pb.getPath());
        }
        if (pb.hasWeight()) {
            query.setWeight(pb.getWeight());
        }
        if (pb.hasQuery()) {
            query.setQuery(toQuery(pb.getQuery()));
        }
        if (pb.hasScoreMode()) {
            query.setScoreMode(toScoreMode(pb.getScoreMode()));
        }
        if (pb.hasInnerHits()) {
            query.setInnerHits(toInnerHits(pb.getInnerHits()));
        }
        return query;
    }

    private static GeoBoundingBoxQuery toGeoBoundingBoxQuery(ByteString queryByteString) throws IOException {
        Search.GeoBoundingBoxQuery pb = Search.GeoBoundingBoxQuery.parseFrom(queryByteString);
        GeoBoundingBoxQuery query = new GeoBoundingBoxQuery();
        if (pb.hasFieldName()) {
            query.setFieldName(pb.getFieldName());
        }
        if (pb.hasBottomRight()) {
            query.setBottomRight(pb.getBottomRight());
        }
        if (pb.hasTopLeft()) {
            query.setTopLeft(pb.getTopLeft());
        }
        return query;
    }

    private static GeoDistanceQuery toGeoDistanceQuery(ByteString queryByteString) throws IOException {
        Search.GeoDistanceQuery pb = Search.GeoDistanceQuery.parseFrom(queryByteString);
        GeoDistanceQuery query = new GeoDistanceQuery();
        if (pb.hasFieldName()) {
            query.setFieldName(pb.getFieldName());
        }
        if (pb.hasCenterPoint()) {
            query.setCenterPoint(pb.getCenterPoint());
        }
        if (pb.hasDistance()) {
            query.setDistanceInMeter(pb.getDistance());
        }
        return query;
    }

    private static GeoPolygonQuery toGeoPolygonQuery(ByteString queryByteString) throws IOException {
        Search.GeoPolygonQuery pb = Search.GeoPolygonQuery.parseFrom(queryByteString);
        GeoPolygonQuery query = new GeoPolygonQuery();
        if (pb.hasFieldName()) {
            query.setFieldName(pb.getFieldName());
        }
        query.setPoints(pb.getPointsList());
        return query;
    }

    private static ExistsQuery toExistsQuery(ByteString queryByteString) throws IOException {
        Search.ExistsQuery pb = Search.ExistsQuery.parseFrom(queryByteString);
        ExistsQuery query = new ExistsQuery();
        if (pb.hasFieldName()) {
            query.setFieldName(pb.getFieldName());
        }
        return query;
    }

    private static KnnVectorQuery toKnnVectorQuery(ByteString queryByteString) throws IOException {
        Search.KnnVectorQuery pb = Search.KnnVectorQuery.parseFrom(queryByteString);
        KnnVectorQuery query = new KnnVectorQuery();
        if (pb.hasFieldName()) {
            query.setFieldName(pb.getFieldName());
        }
        if (pb.hasTopK()) {
            query.setTopK(pb.getTopK());
        }
        List<Float> floatList = pb.getFloat32QueryVectorList();
        float[] floats = new float[floatList.size()];
        for (int i = 0; i < floats.length; i++) {
            floats[i] = floatList.get(i);
        }
        query.setFloat32QueryVector(floats);
        if (pb.hasFilter()) {
            query.setFilter(toQuery(pb.getFilter()));
        }
        if (pb.hasWeight()) {
            query.setWeight(pb.getWeight());
        }
        return query;
    }
}
