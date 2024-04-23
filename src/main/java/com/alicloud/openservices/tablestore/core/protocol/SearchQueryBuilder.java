package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.google.protobuf.ByteString;

import static com.alicloud.openservices.tablestore.core.protocol.SearchInnerHitsBuilder.buildInnerHits;

/**
 * {@link Query} serialization tool class. For deserialization, please refer to {@link SearchQueryParser}
 */
public class SearchQueryBuilder {

    private static Search.QueryType buildQueryType(QueryType type) {
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
            case QueryType_SuffixQuery:
                return Search.QueryType.SUFFIX_QUERY;
            case QueryType_BoolQuery:
                return Search.QueryType.BOOL_QUERY;
            case QueryType_ConstScoreQuery:
                return Search.QueryType.CONST_SCORE_QUERY;
            case QueryType_FunctionScoreQuery:
                return Search.QueryType.FUNCTION_SCORE_QUERY;
            case QueryType_FunctionsScoreQuery:
                return Search.QueryType.FUNCTIONS_SCORE_QUERY;
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
            case QueryType_ExistsQuery:
                return Search.QueryType.EXISTS_QUERY;
            case QueryType_KnnVectorQuery:
                return Search.QueryType.KNN_VECTOR_QUERY;
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

    public static byte[] buildQueryToBytes(Query query) {
        return buildQuery(query).toByteArray();
    }

    public static Search.MatchAllQuery buildMatchAllQuery() {
        Search.MatchAllQuery.Builder builder = Search.MatchAllQuery.newBuilder();
        return builder.build();
    }

    public static Search.MatchQuery buildMatchQuery(MatchQuery query) {
        Search.MatchQuery.Builder builder = Search.MatchQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        builder.setText(query.getText());
        builder.setWeight(query.getWeight());
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
        builder.setWeight(query.getWeight());
        return builder.build();
    }

    public static Search.TermQuery buildTermQuery(TermQuery query) {
        Search.TermQuery.Builder builder = Search.TermQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        builder.setTerm(ByteString.copyFrom(SearchVariantType.toVariant(query.getTerm())));
        builder.setWeight(query.getWeight());
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
        builder.setWeight(query.getWeight());
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
        builder.setWeight(query.getWeight());
        return builder.build();
    }

    public static Search.SuffixQuery buildSuffixQuery(SuffixQuery query) {
        Search.SuffixQuery.Builder builder = Search.SuffixQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        builder.setSuffix(query.getSuffix());
        builder.setWeight(query.getWeight());
        return builder.build();
    }

    public static Search.WildcardQuery buildWildcardQuery(WildcardQuery query) {
        Search.WildcardQuery.Builder builder = Search.WildcardQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        builder.setValue(query.getValue());
        builder.setWeight(query.getWeight());
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

    private static Search.FieldValueFactor buildFieldValueFactor(FieldValueFactor fieldValueFactor) {
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

    private static Search.FunctionModifier buildModifier(FieldValueFactorFunction.FunctionModifier modifier) {
        switch (modifier) {
            case NONE:
                return Search.FunctionModifier.FM_NONE;
            case LOG:
                return Search.FunctionModifier.FM_LOG;
            case LOG1P:
                return Search.FunctionModifier.FM_LOG1P;
            case LOG2P:
                return Search.FunctionModifier.FM_LOG2P;
            case LN:
                return Search.FunctionModifier.FM_LN;
            case LN1P:
                return Search.FunctionModifier.FM_LN1P;
            case LN2P:
                return Search.FunctionModifier.FM_LN2P;
            case SQUARE:
                return Search.FunctionModifier.FM_SQUARE;
            case SQRT:
                return Search.FunctionModifier.FM_SQRT;
            case RECIPROCAL:
                return Search.FunctionModifier.FM_RECIPROCAL;
            default:
                throw new IllegalArgumentException("unknown modifier: " + modifier.name());
        }
    }

    private static Search.DecayMathFunction buildDecayMathFunction(DecayFunction.MathFunction mathFunction) {
        switch (mathFunction) {
            case GAUSS:
                return Search.DecayMathFunction.GAUSS;
            case EXP:
                return Search.DecayMathFunction.EXP;
            case LINEAR:
                return Search.DecayMathFunction.LINEAR;
            default:
                throw new IllegalArgumentException("unknown MathFunction: " + mathFunction.name());
        }
    }

    private static Search.MultiValueMode buildMultiValueMode(MultiValueMode multiValueMode) {
        switch (multiValueMode) {
            case MIN:
                return Search.MultiValueMode.MVM_MIN;
            case MAX:
                return Search.MultiValueMode.MVM_MAX;
            case AVG:
                return Search.MultiValueMode.MVM_AVG;
            case SUM:
                return Search.MultiValueMode.MVM_SUM;
            default:
                throw new IllegalArgumentException("unknown MultiValueMode: " + multiValueMode.name());
        }
    }

    private static Search.FunctionScoreMode buildFunctionScoreMode(FunctionsScoreQuery.ScoreMode scoreMode) {
        if (scoreMode == null) {
            return null;
        }
        switch (scoreMode) {
            case AVG:
                return Search.FunctionScoreMode.FSM_AVG;
            case MAX:
                return Search.FunctionScoreMode.FSM_MAX;
            case SUM:
                return Search.FunctionScoreMode.FSM_SUM;
            case MIN:
                return Search.FunctionScoreMode.FSM_MIN;
            case MULTIPLY:
                return Search.FunctionScoreMode.FSM_MULTIPLY;
            case FIRST:
                return Search.FunctionScoreMode.FSM_FIRST;
            default:
                throw new IllegalArgumentException("unknown FunctionsScoreQuery.ScoreMode: " + scoreMode.name());
        }
    }

    private static Search.FunctionCombineMode buildCombineMode(FunctionsScoreQuery.CombineMode combineMode) {
        switch (combineMode) {
            case MULTIPLY:
                return Search.FunctionCombineMode.FCM_MULTIPLY;
            case AVG:
                return Search.FunctionCombineMode.FCM_AVG;
            case MAX:
                return Search.FunctionCombineMode.FCM_MAX;
            case SUM:
                return Search.FunctionCombineMode.FCM_SUM;
            case MIN:
                return Search.FunctionCombineMode.FCM_MIN;
            case REPLACE:
                return Search.FunctionCombineMode.FCM_REPLACE;
            default:
                throw new IllegalArgumentException("unknown FunctionsScoreQuery.CombineMode: " + combineMode.name());
        }
    }

    private static Search.Function buildScoreFunction(ScoreFunction function) {
        Search.Function.Builder builder = Search.Function.newBuilder();
        if (function.getWeight() != null) {
            builder.setWeight(function.getWeight());
        }
        if (function.getFilter() != null) {
            builder.setFilter(buildQuery(function.getFilter()));
        }

        if (function.getFieldValueFactorFunction() != null) {
            Search.FieldValueFactorFunction.Builder fieldValueFactorBuilder = Search.FieldValueFactorFunction.newBuilder();
            if (function.getFieldValueFactorFunction().getFieldName() != null) {
                fieldValueFactorBuilder.setFieldName(function.getFieldValueFactorFunction().getFieldName());
            }
            if (function.getFieldValueFactorFunction().getFactor() != null) {
                fieldValueFactorBuilder.setFactor(function.getFieldValueFactorFunction().getFactor());
            }
            if (function.getFieldValueFactorFunction().getModifier() != null) {
                fieldValueFactorBuilder.setModifier(buildModifier(function.getFieldValueFactorFunction().getModifier()));
            }
            if (function.getFieldValueFactorFunction().getMissing() != null) {
                fieldValueFactorBuilder.setMissing(function.getFieldValueFactorFunction().getMissing());
            }
            builder.setFieldValueFactor(fieldValueFactorBuilder);
        }
        if (function.getDecayFunction() != null) {
            if (function.getDecayFunction().getDecayParam() == null) {
                throw new IllegalArgumentException("decayParam is empty");
            }
            DecayParam.ParamType decayParamType = function.getDecayFunction().getDecayParam().getType();
            if (decayParamType == null) {
                throw new IllegalArgumentException("decayParamType is empty");
            }
            Search.DecayFuncParamType pbDecayParamType;
            ByteString pbDecayParam;
            switch (decayParamType) {
                case DATE:
                    pbDecayParamType = Search.DecayFuncParamType.DF_DATE_PARAM;
                    DecayFuncDateParam decayFuncDateParam = (DecayFuncDateParam) function.getDecayFunction().getDecayParam();
                    Search.DecayFuncDateParam.Builder decayDateParamBuilder = Search.DecayFuncDateParam.newBuilder();
                    if (decayFuncDateParam.getOriginLong() != null) {
                        decayDateParamBuilder.setOriginLong(decayFuncDateParam.getOriginLong());
                    }
                    if (decayFuncDateParam.getOriginString() != null) {
                        decayDateParamBuilder.setOriginString(decayFuncDateParam.getOriginString());
                    }
                    if (decayFuncDateParam.getScale() != null) {
                        decayDateParamBuilder.setScale(SearchProtocolBuilder.buildDateTimeValue(decayFuncDateParam.getScale()));
                    }
                    if (decayFuncDateParam.getOffset() != null) {
                        decayDateParamBuilder.setOffset(SearchProtocolBuilder.buildDateTimeValue(decayFuncDateParam.getOffset()));
                    }
                    pbDecayParam = decayDateParamBuilder.build().toByteString();
                    break;
                case GEO:
                    pbDecayParamType = Search.DecayFuncParamType.DF_GEO_PARAM;
                    DecayFuncGeoParam decayFuncGeoParam = (DecayFuncGeoParam) function.getDecayFunction().getDecayParam();
                    Search.DecayFuncGeoParam.Builder decayGeoParamBuilder = Search.DecayFuncGeoParam.newBuilder();
                    if (decayFuncGeoParam.getOrigin() != null) {
                        decayGeoParamBuilder.setOrigin(decayFuncGeoParam.getOrigin());
                    }
                    if (decayFuncGeoParam.getScale() != null) {
                        decayGeoParamBuilder.setScale(decayFuncGeoParam.getScale());
                    }
                    if (decayFuncGeoParam.getOffset() != null) {
                        decayGeoParamBuilder.setOffset(decayFuncGeoParam.getOffset());
                    }
                    pbDecayParam = decayGeoParamBuilder.build().toByteString();
                    break;
                case NUMERIC:
                    pbDecayParamType = Search.DecayFuncParamType.DF_NUMERIC_PARAM;
                    DecayFuncNumericParam decayFuncNumericParam = (DecayFuncNumericParam) function.getDecayFunction().getDecayParam();
                    Search.DecayFuncNumericParam.Builder decayNumericParamBuilder = Search.DecayFuncNumericParam.newBuilder();
                    if (decayFuncNumericParam.getOrigin() != null) {
                        decayNumericParamBuilder.setOrigin(decayFuncNumericParam.getOrigin());
                    }
                    if (decayFuncNumericParam.getScale() != null) {
                        decayNumericParamBuilder.setScale(decayFuncNumericParam.getScale());
                    }
                    if (decayFuncNumericParam.getOffset() != null) {
                        decayNumericParamBuilder.setOffset(decayFuncNumericParam.getOffset());
                    }
                    pbDecayParam = decayNumericParamBuilder.build().toByteString();
                    break;
                default:
                    throw new IllegalArgumentException("unknown decayParamType: " + decayParamType.name());
            }

            Search.DecayFunction.Builder decayFunctionBuilder = Search.DecayFunction.newBuilder();
            if (function.getDecayFunction().getFieldName() != null) {
                decayFunctionBuilder.setFieldName(function.getDecayFunction().getFieldName());
            }
            if (function.getDecayFunction().getMathFunction() != null) {
                decayFunctionBuilder.setMathFunction(buildDecayMathFunction(function.getDecayFunction().getMathFunction()));
            }
            decayFunctionBuilder.setParamType(pbDecayParamType);
            if (pbDecayParam != null) {
                decayFunctionBuilder.setParam(pbDecayParam);
            }
            if (function.getDecayFunction().getDecay() != null) {
                decayFunctionBuilder.setDecay(function.getDecayFunction().getDecay());
            }
            if (function.getDecayFunction().getMultiValueMode() != null) {
                decayFunctionBuilder.setMultiValueMode(buildMultiValueMode(function.getDecayFunction().getMultiValueMode()));
            }
            builder.setDecay(decayFunctionBuilder);
        }
        if (function.getRandomFunction() != null) {
            builder.setRandom(Search.RandomScoreFunction.newBuilder());
        }
        return builder.build();
    }

    public static Search.FunctionsScoreQuery buildFunctionsScoreQuery(FunctionsScoreQuery query) {
        Search.FunctionsScoreQuery.Builder builder = Search.FunctionsScoreQuery.newBuilder();
        if (query.getQuery() != null) {
            builder.setQuery(SearchQueryBuilder.buildQuery(query.getQuery()));
        }
        if (query.getScoreMode() != null) {
            builder.setScoreMode(buildFunctionScoreMode(query.getScoreMode()));
        }
        if (query.getCombineMode() != null) {
            builder.setCombineMode(buildCombineMode(query.getCombineMode()));
        }
        if (query.getMinScore() != null) {
            builder.setMinScore(query.getMinScore());
        }
        if (query.getMaxScore() != null) {
            builder.setMaxScore(query.getMaxScore());
        }
        if (query.getFunctions() != null) {
            for (ScoreFunction function : query.getFunctions()) {
                builder.addFunctions(buildScoreFunction(function));
            }
        }
        return builder.build();
    }

    private static Search.ScoreMode buildScoreMode(ScoreMode scoreMode) {
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
        builder.setWeight(query.getWeight());
        if (query.getScoreMode() == null) {
            throw new IllegalArgumentException("nestedQuery must set score mode.");
        }
        builder.setScoreMode(buildScoreMode(query.getScoreMode()));
        if (query.getInnerHits() != null) {
            builder.setInnerHits(buildInnerHits(query.getInnerHits()));
        }
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

    public static Search.ExistsQuery buildExistsQuery(ExistsQuery query) {
        Search.ExistsQuery.Builder builder = Search.ExistsQuery.newBuilder();
        builder.setFieldName(query.getFieldName());
        return builder.build();
    }

    public static Search.KnnVectorQuery buildKnnVectorQuery(KnnVectorQuery query) {
        Search.KnnVectorQuery.Builder builder = Search.KnnVectorQuery.newBuilder();
        if (query.getFieldName() != null) {
            builder.setFieldName(query.getFieldName());
        }
        if (query.getTopK() != null) {
            builder.setTopK(query.getTopK());
        }
        if (query.getFloat32QueryVector() != null) {
            for (float v : query.getFloat32QueryVector()) {
                builder.addFloat32QueryVector(v);
            }
        }
        if (query.getFilter() != null) {
            builder.setFilter(buildQuery(query.getFilter()));
        }
        if (query.getWeight() != null) {
            builder.setWeight(query.getWeight());
        }
        return builder.build();
    }
}
