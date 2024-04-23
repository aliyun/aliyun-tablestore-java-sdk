package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.ConstScoreQuery;
import com.alicloud.openservices.tablestore.model.search.query.DecayFuncDateParam;
import com.alicloud.openservices.tablestore.model.search.query.DecayFuncGeoParam;
import com.alicloud.openservices.tablestore.model.search.query.DecayFuncNumericParam;
import com.alicloud.openservices.tablestore.model.search.query.DecayFunction;
import com.alicloud.openservices.tablestore.model.search.query.DecayParam;
import com.alicloud.openservices.tablestore.model.search.query.ExistsQuery;
import com.alicloud.openservices.tablestore.model.search.query.FieldValueFactor;
import com.alicloud.openservices.tablestore.model.search.query.FieldValueFactorFunction;
import com.alicloud.openservices.tablestore.model.search.query.FunctionScoreQuery;
import com.alicloud.openservices.tablestore.model.search.query.FunctionsScoreQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoBoundingBoxQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoDistanceQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoPolygonQuery;
import com.alicloud.openservices.tablestore.model.search.query.KnnVectorQuery;
import com.alicloud.openservices.tablestore.model.search.query.MatchAllQuery;
import com.alicloud.openservices.tablestore.model.search.query.MatchPhraseQuery;
import com.alicloud.openservices.tablestore.model.search.query.MatchQuery;
import com.alicloud.openservices.tablestore.model.search.query.MultiValueMode;
import com.alicloud.openservices.tablestore.model.search.query.NestedQuery;
import com.alicloud.openservices.tablestore.model.search.query.PrefixQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.QueryOperator;
import com.alicloud.openservices.tablestore.model.search.query.RandomFunction;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;
import com.alicloud.openservices.tablestore.model.search.query.ScoreFunction;
import com.alicloud.openservices.tablestore.model.search.query.ScoreMode;
import com.alicloud.openservices.tablestore.model.search.query.SuffixQuery;
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
            case SUFFIX_QUERY:
                return toSuffixQuery(queryByteString);
            case BOOL_QUERY:
                return toBoolQuery(queryByteString);
            case CONST_SCORE_QUERY:
                return toConstScoreQuery(queryByteString);
            case FUNCTION_SCORE_QUERY:
                return toFunctionScoreQuery(queryByteString);
            case FUNCTIONS_SCORE_QUERY:
                return toFunctionsScoreQuery(queryByteString);
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

    private static SuffixQuery toSuffixQuery(ByteString queryByteString) throws IOException {
        Search.SuffixQuery pb = Search.SuffixQuery.parseFrom(queryByteString);
        SuffixQuery query = new SuffixQuery();
        if (pb.hasFieldName()) {
            query.setFieldName(pb.getFieldName());
        }
        if (pb.hasWeight()) {
            query.setWeight(pb.getWeight());
        }
        if (pb.hasSuffix()) {
            query.setSuffix(pb.getSuffix());
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

    private static FunctionsScoreQuery.ScoreMode toFunctionScoreMode(Search.FunctionScoreMode scoreMode) {
        switch (scoreMode) {
            case FSM_AVG:
                return FunctionsScoreQuery.ScoreMode.AVG;
            case FSM_MAX:
                return FunctionsScoreQuery.ScoreMode.MAX;
            case FSM_SUM:
                return FunctionsScoreQuery.ScoreMode.SUM;
            case FSM_MIN:
                return FunctionsScoreQuery.ScoreMode.MIN;
            case FSM_MULTIPLY:
                return FunctionsScoreQuery.ScoreMode.MULTIPLY;
            case FSM_FIRST:
                return FunctionsScoreQuery.ScoreMode.FIRST;
            default:
                return FunctionsScoreQuery.ScoreMode.UNKNOWN;
        }
    }

    private static FunctionsScoreQuery.CombineMode toCombineMode(Search.FunctionCombineMode combineMode) {
        switch (combineMode) {
            case FCM_MULTIPLY:
                return FunctionsScoreQuery.CombineMode.MULTIPLY;
            case FCM_AVG:
                return FunctionsScoreQuery.CombineMode.AVG;
            case FCM_MAX:
                return FunctionsScoreQuery.CombineMode.MAX;
            case FCM_SUM:
                return FunctionsScoreQuery.CombineMode.SUM;
            case FCM_MIN:
                return FunctionsScoreQuery.CombineMode.MIN;
            case FCM_REPLACE:
                return FunctionsScoreQuery.CombineMode.REPLACE;
            default:
                return FunctionsScoreQuery.CombineMode.UNKNOWN;
        }
    }

    private static DecayFunction.MathFunction toDecayMathFunction(Search.DecayMathFunction mathFunction) {
        switch (mathFunction) {
            case GAUSS:
                return DecayFunction.MathFunction.GAUSS;
            case EXP:
                return DecayFunction.MathFunction.EXP;
            case LINEAR:
                return DecayFunction.MathFunction.LINEAR;
            default:
                return DecayFunction.MathFunction.UNKNOWN;
        }
    }

    private static MultiValueMode toMultiValueMode(Search.MultiValueMode multiValueMode) {
        switch (multiValueMode) {
            case MVM_MIN:
                return MultiValueMode.MIN;
            case MVM_MAX:
                return MultiValueMode.MAX;
            case MVM_AVG:
                return MultiValueMode.AVG;
            case MVM_SUM:
                return MultiValueMode.SUM;
            default:
                return MultiValueMode.UNKNOWN;
        }
    }

    private static FieldValueFactorFunction.FunctionModifier toModifier(Search.FunctionModifier modifier) {
        switch (modifier) {
            case FM_NONE:
                return FieldValueFactorFunction.FunctionModifier.NONE;
            case FM_LOG:
                return FieldValueFactorFunction.FunctionModifier.LOG;
            case FM_LOG1P:
                return FieldValueFactorFunction.FunctionModifier.LOG1P;
            case FM_LOG2P:
                return FieldValueFactorFunction.FunctionModifier.LOG2P;
            case FM_LN:
                return FieldValueFactorFunction.FunctionModifier.LN;
            case FM_LN1P:
                return FieldValueFactorFunction.FunctionModifier.LN1P;
            case FM_LN2P:
                return FieldValueFactorFunction.FunctionModifier.LN2P;
            case FM_SQUARE:
                return FieldValueFactorFunction.FunctionModifier.SQUARE;
            case FM_SQRT:
                return FieldValueFactorFunction.FunctionModifier.SQRT;
            case FM_RECIPROCAL:
                return FieldValueFactorFunction.FunctionModifier.RECIPROCAL;
            default:
                return FieldValueFactorFunction.FunctionModifier.UNKNOWN;
        }
    }

    private static ScoreFunction toScoreFunction(Search.Function pb) throws IOException {
        ScoreFunction scoreFunction = new ScoreFunction();
        if (pb.hasFilter()) {
            scoreFunction.setFilter(toQuery(pb.getFilter()));
        }
        if (pb.hasWeight()) {
            scoreFunction.setWeight(pb.getWeight());
        }

        if (pb.hasFieldValueFactor()) {
            Search.FieldValueFactorFunction pbFvfFunction = pb.getFieldValueFactor();
            FieldValueFactorFunction fvfFunction = new FieldValueFactorFunction();
            if (pbFvfFunction.hasFieldName()) {
                fvfFunction.setFieldName(pbFvfFunction.getFieldName());
            }
            if (pbFvfFunction.hasFactor()) {
                fvfFunction.setFactor(pbFvfFunction.getFactor());
            }
            if (pbFvfFunction.hasModifier()) {
                fvfFunction.setModifier(toModifier(pbFvfFunction.getModifier()));
            }
            if (pbFvfFunction.hasMissing()) {
                fvfFunction.setMissing(pbFvfFunction.getMissing());
            }
            scoreFunction.setFieldValueFactorFunction(fvfFunction);
        }
        if (pb.hasDecay()) {
            Search.DecayFunction pbDFunction = pb.getDecay();
            DecayFunction dFunction = new DecayFunction();
            if (pbDFunction.hasParamType() && pbDFunction.hasParam()) {
                Search.DecayFuncParamType decayParamType = pbDFunction.getParamType();
                switch (decayParamType) {
                    case DF_DATE_PARAM:
                        DecayFuncDateParam decayFuncDateParam = new DecayFuncDateParam();
                        Search.DecayFuncDateParam pbDateParam = Search.DecayFuncDateParam.parseFrom(pbDFunction.getParam().toByteArray());
                        if (pbDateParam.hasOriginString()) {
                            decayFuncDateParam.setOriginString(pbDateParam.getOriginString());
                        }
                        if (pbDateParam.hasOriginLong()) {
                            decayFuncDateParam.setOriginLong(pbDateParam.getOriginLong());
                        }
                        if (pbDateParam.hasScale()) {
                            decayFuncDateParam.setScale(SearchProtocolParser.toDateTimeValue(pbDateParam.getScale()));
                        }
                        if (pbDateParam.hasOffset()) {
                            decayFuncDateParam.setOffset(SearchProtocolParser.toDateTimeValue(pbDateParam.getOffset()));
                        }
                        dFunction.setDecayParam(decayFuncDateParam);
                        break;
                    case DF_GEO_PARAM:
                        DecayFuncGeoParam decayFuncGeoParam = new DecayFuncGeoParam();
                        Search.DecayFuncGeoParam pbGeoParam = Search.DecayFuncGeoParam.parseFrom(pbDFunction.getParam().toByteArray());
                        if (pbGeoParam.hasOrigin()) {
                            decayFuncGeoParam.setOrigin(pbGeoParam.getOrigin());
                        }
                        if (pbGeoParam.hasScale()) {
                            decayFuncGeoParam.setScale(pbGeoParam.getScale());
                        }
                        if (pbGeoParam.hasOffset()) {
                            decayFuncGeoParam.setOffset(pbGeoParam.getOffset());
                        }
                        dFunction.setDecayParam(decayFuncGeoParam);
                        break;
                    case DF_NUMERIC_PARAM:
                        DecayFuncNumericParam decayFuncNumericParam = new DecayFuncNumericParam();
                        Search.DecayFuncNumericParam pbDoubleParam = Search.DecayFuncNumericParam.parseFrom(pbDFunction.getParam().toByteArray());
                        if (pbDoubleParam.hasOrigin()) {
                            decayFuncNumericParam.setOrigin(pbDoubleParam.getOrigin());
                        }
                        if (pbDoubleParam.hasScale()) {
                            decayFuncNumericParam.setScale(pbDoubleParam.getScale());
                        }
                        if (pbDoubleParam.hasOffset()) {
                            decayFuncNumericParam.setOffset(pbDoubleParam.getOffset());
                        }
                        dFunction.setDecayParam(decayFuncNumericParam);
                        break;
                    default:
                        dFunction.setDecayParam(DecayParam.unknownTypeParam());
                }
            }
            if (pbDFunction.hasFieldName()) {
                dFunction.setFieldName(pbDFunction.getFieldName());
            }
            if (pbDFunction.hasMathFunction()) {
                dFunction.setMathFunction(toDecayMathFunction(pbDFunction.getMathFunction()));
            }
            if (pbDFunction.hasDecay()) {
                dFunction.setDecay(pbDFunction.getDecay());
            }
            if (pbDFunction.hasMultiValueMode()) {
                dFunction.setMultiValueMode(toMultiValueMode(pbDFunction.getMultiValueMode()));
            }
            scoreFunction.setDecayFunction(dFunction);
        }
        if (pb.hasRandom()) {
            scoreFunction.setRandomFunction(new RandomFunction());
        }
        return scoreFunction;
    }

    private static Query toFunctionsScoreQuery(ByteString queryByteString) throws IOException {
        Search.FunctionsScoreQuery pb = Search.FunctionsScoreQuery.parseFrom(queryByteString);
        FunctionsScoreQuery query = new FunctionsScoreQuery();
        if (pb.hasQuery()) {
            query.setQuery(toQuery(pb.getQuery()));
        }
        List<ScoreFunction> functions = new ArrayList<ScoreFunction>();
        for(Search.Function function : pb.getFunctionsList()) {
            functions.add(toScoreFunction(function));
        }
        query.setFunctions(functions);
        if (pb.hasScoreMode()) {
            query.setScoreMode(toFunctionScoreMode(pb.getScoreMode()));
        }
        if (pb.hasCombineMode()) {
            query.setCombineMode(toCombineMode(pb.getCombineMode()));
        }
        if (pb.hasMinScore()) {
            query.setMinScore(pb.getMinScore());
        }
        if (pb.hasMaxScore()) {
            query.setMaxScore(pb.getMaxScore());
        }
        return query;
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
