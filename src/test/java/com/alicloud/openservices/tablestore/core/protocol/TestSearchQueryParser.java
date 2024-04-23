package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.search.DateTimeUnit;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.alicloud.openservices.tablestore.model.search.DateTimeValue;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Test;

import java.io.IOException;

import static com.alicloud.openservices.tablestore.core.protocol.SearchQueryParser.toQuery;
import static org.junit.Assert.assertEquals;

public class TestSearchQueryParser extends BaseSearchTest {

    private static final Gson GSON = new GsonBuilder()
            .disableHtmlEscaping()
            .serializeNulls()
            .serializeSpecialFloatingPointValues()
            .enableComplexMapKeySerialization()
            .setExclusionStrategies(new ExclusionStrategy() {
                @Override
                public boolean shouldSkipField(FieldAttributes f) {
                    return "rawData".equals(f.getName())
                            && "[B".equals(f.getDeclaredClass().getName())
                            && f.getDeclaringClass().equals(PrimaryKeyValue.class);
                }
                @Override
                public boolean shouldSkipClass(Class<?> aClass) {
                    return false;
                }
            })
            .create();

    @Test
    public void testToFunctionScoreMode() throws IOException {
        Search.FunctionScoreMode[] pbModes = new Search.FunctionScoreMode[]{Search.FunctionScoreMode.FSM_AVG, Search.FunctionScoreMode.FSM_MAX, Search.FunctionScoreMode.FSM_SUM,
                Search.FunctionScoreMode.FSM_MIN, Search.FunctionScoreMode.FSM_MULTIPLY, Search.FunctionScoreMode.FSM_FIRST};
        FunctionsScoreQuery.ScoreMode[] modes = new FunctionsScoreQuery.ScoreMode[]{FunctionsScoreQuery.ScoreMode.AVG, FunctionsScoreQuery.ScoreMode.MAX, FunctionsScoreQuery.ScoreMode.SUM,
                FunctionsScoreQuery.ScoreMode.MIN, FunctionsScoreQuery.ScoreMode.MULTIPLY, FunctionsScoreQuery.ScoreMode.FIRST};
        for (int i = 0; i < 6; i++) {
            Query query = toQuery(Search.Query.newBuilder()
                    .setType(Search.QueryType.FUNCTIONS_SCORE_QUERY)
                    .setQuery(Search.FunctionsScoreQuery.newBuilder()
                            .setScoreMode(pbModes[i])
                            .build().toByteString())
                    .build());
            assertEquals(modes[i], ((FunctionsScoreQuery)query).getScoreMode());
        }
    }

    @Test
    public void testToCombineMode() throws IOException {
        Search.FunctionCombineMode[] pbModes = new Search.FunctionCombineMode[]{Search.FunctionCombineMode.FCM_MULTIPLY, Search.FunctionCombineMode.FCM_AVG,
                Search.FunctionCombineMode.FCM_MAX, Search.FunctionCombineMode.FCM_SUM, Search.FunctionCombineMode.FCM_MIN, Search.FunctionCombineMode.FCM_REPLACE};
        FunctionsScoreQuery.CombineMode[] modes = new FunctionsScoreQuery.CombineMode[]{FunctionsScoreQuery.CombineMode.MULTIPLY, FunctionsScoreQuery.CombineMode.AVG,
                FunctionsScoreQuery.CombineMode.MAX, FunctionsScoreQuery.CombineMode.SUM, FunctionsScoreQuery.CombineMode.MIN, FunctionsScoreQuery.CombineMode.REPLACE};
        for (int i = 0; i < 6; i++) {
            Query query = toQuery(Search.Query.newBuilder()
                   .setType(Search.QueryType.FUNCTIONS_SCORE_QUERY)
                   .setQuery(Search.FunctionsScoreQuery.newBuilder()
                           .setCombineMode(pbModes[i])
                           .build().toByteString())
                   .build());
            assertEquals(modes[i], ((FunctionsScoreQuery)query).getCombineMode());
        }
    }

    @Test
    public void testToDecayMathFunction() throws IOException {
        Search.DecayMathFunction[] pbMathFuncs = new Search.DecayMathFunction[]{Search.DecayMathFunction.GAUSS, Search.DecayMathFunction.EXP, Search.DecayMathFunction.LINEAR};
        DecayFunction.MathFunction[] mathFuncs = new DecayFunction.MathFunction[]{DecayFunction.MathFunction.GAUSS, DecayFunction.MathFunction.EXP, DecayFunction.MathFunction.LINEAR};
        for (int i = 0; i < 3; i++) {
            Query query = toQuery(Search.Query.newBuilder()
                    .setType(Search.QueryType.FUNCTIONS_SCORE_QUERY)
                    .setQuery(Search.FunctionsScoreQuery.newBuilder()
                            .addFunctions(Search.Function.newBuilder()
                                    .setDecay(Search.DecayFunction.newBuilder()
                                            .setMathFunction(pbMathFuncs[i])
                                            .build()))
                            .build().toByteString())
                    .build());
            assertEquals(mathFuncs[i], ((FunctionsScoreQuery)query).getFunctions().get(0).getDecayFunction().getMathFunction());
        }
    }

    @Test
    public void testToDecayMultiValueMode() throws IOException {
        Search.MultiValueMode[] pbMultiValueModes = new Search.MultiValueMode[]{Search.MultiValueMode.MVM_MAX, Search.MultiValueMode.MVM_MIN, Search.MultiValueMode.MVM_SUM,
                Search.MultiValueMode.MVM_AVG};
        MultiValueMode[] multiValueModes = new MultiValueMode[]{MultiValueMode.MAX, MultiValueMode.MIN, MultiValueMode.SUM,
                MultiValueMode.AVG};
        for (int i = 0; i < 4; i++) {
            Query query = toQuery(Search.Query.newBuilder()
                   .setType(Search.QueryType.FUNCTIONS_SCORE_QUERY)
                   .setQuery(Search.FunctionsScoreQuery.newBuilder()
                           .addFunctions(Search.Function.newBuilder()
                                   .setDecay(Search.DecayFunction.newBuilder()
                                           .setMultiValueMode(pbMultiValueModes[i])
                                           .build()))
                           .build().toByteString())
                    .build());
            assertEquals(multiValueModes[i], ((FunctionsScoreQuery)query).getFunctions().get(0).getDecayFunction().getMultiValueMode());
       }
   }

    @Test
    public void testToModifier() throws IOException {
        Search.FunctionModifier[] pbModifiers = new Search.FunctionModifier[]{Search.FunctionModifier.FM_NONE, Search.FunctionModifier.FM_LOG, Search.FunctionModifier.FM_LOG1P, Search.FunctionModifier.FM_LOG2P,
                Search.FunctionModifier.FM_LN, Search.FunctionModifier.FM_LN1P, Search.FunctionModifier.FM_LN2P, Search.FunctionModifier.FM_SQUARE, Search.FunctionModifier.FM_SQRT,
                Search.FunctionModifier.FM_RECIPROCAL};
        FieldValueFactorFunction.FunctionModifier[] modifiers = new FieldValueFactorFunction.FunctionModifier[]{FieldValueFactorFunction.FunctionModifier.NONE, FieldValueFactorFunction.FunctionModifier.LOG, FieldValueFactorFunction.FunctionModifier.LOG1P,
                FieldValueFactorFunction.FunctionModifier.LOG2P, FieldValueFactorFunction.FunctionModifier.LN, FieldValueFactorFunction.FunctionModifier.LN1P, FieldValueFactorFunction.FunctionModifier.LN2P, FieldValueFactorFunction.FunctionModifier.SQUARE, FieldValueFactorFunction.FunctionModifier.SQRT,
                FieldValueFactorFunction.FunctionModifier.RECIPROCAL};
        for (int i = 0; i < 10; i++) {
            Query query = toQuery(Search.Query.newBuilder()
                   .setType(Search.QueryType.FUNCTIONS_SCORE_QUERY)
                   .setQuery(Search.FunctionsScoreQuery.newBuilder()
                           .addFunctions(Search.Function.newBuilder()
                                   .setFieldValueFactor(Search.FieldValueFactorFunction.newBuilder()
                                           .setModifier(pbModifiers[i])
                                           .build()))
                           .build().toByteString())
                   .build());
            assertEquals(modifiers[i], ((FunctionsScoreQuery)query).getFunctions().get(0).getFieldValueFactorFunction().getModifier());
        }
    }

    @Test
    public void testToFunctionsScoreQuery() throws IOException {
        Query actual = toQuery(Search.Query.newBuilder()
                .setType(Search.QueryType.FUNCTIONS_SCORE_QUERY)
                .setQuery(Search.FunctionsScoreQuery.newBuilder()
                        .setQuery(SearchQueryBuilder.buildQuery(QueryBuilders.matchAll().build()))
                        .addFunctions(Search.Function.newBuilder()
                                .setFieldValueFactor(Search.FieldValueFactorFunction.newBuilder()
                                        .setFieldName("field")
                                        .setFactor(1)
                                        .setMissing(1)
                                        .setModifier(Search.FunctionModifier.FM_LN1P)
                                        .build()))
                        .addFunctions(Search.Function.newBuilder()
                                .setDecay(Search.DecayFunction.newBuilder()
                                        .setFieldName("field")
                                        .setMathFunction(Search.DecayMathFunction.GAUSS)
                                        .setDecay(0.5)
                                        .setParam(Search.DecayFuncGeoParam.newBuilder()
                                                .setOrigin("1,1")
                                                .setScale(10)
                                                .setOffset(0)
                                                .build().toByteString())
                                        .setParamType(Search.DecayFuncParamType.DF_GEO_PARAM)
                                        .setMultiValueMode(Search.MultiValueMode.MVM_AVG)
                                        )
                                .setWeight(2)
                                .setFilter(Search.Query.newBuilder()
                                        .setType(Search.QueryType.MATCH_ALL_QUERY)
                                        .setQuery(Search.MatchAllQuery.newBuilder().build().toByteString())
                                        .build()))
                        .addFunctions(Search.Function.newBuilder()
                                .setDecay(Search.DecayFunction.newBuilder()
                                        .setFieldName("field")
                                        .setMathFunction(Search.DecayMathFunction.GAUSS)
                                        .setDecay(0.5)
                                        .setParam(Search.DecayFuncDateParam.newBuilder()
                                                .setOriginString("1970-01-01 00:00:00.000")
                                                .setOriginLong(20L)
                                                .setScale(Search.DateTimeValue.newBuilder()
                                                        .setValue(1)
                                                        .setUnit(Search.DateTimeUnit.MINUTE))
                                                .setOffset(Search.DateTimeValue.newBuilder()
                                                        .setValue(1)
                                                        .setUnit(Search.DateTimeUnit.MINUTE))
                                                .build().toByteString())
                                        .setParamType(Search.DecayFuncParamType.DF_DATE_PARAM)
                                        .setMultiValueMode(Search.MultiValueMode.MVM_AVG)
                                )
                                .setWeight(2)
                                .setFilter(Search.Query.newBuilder()
                                        .setType(Search.QueryType.MATCH_ALL_QUERY)
                                        .setQuery(Search.MatchAllQuery.newBuilder().build().toByteString())
                                        .build()))
                        .addFunctions(Search.Function.newBuilder()
                                .setDecay(Search.DecayFunction.newBuilder()
                                        .setFieldName("field")
                                        .setMathFunction(Search.DecayMathFunction.GAUSS)
                                        .setDecay(0.5)
                                        .setParam(Search.DecayFuncNumericParam.newBuilder()
                                                .setOrigin(2)
                                                .setScale(1)
                                                .setOffset(0)
                                                .build().toByteString())
                                        .setParamType(Search.DecayFuncParamType.DF_NUMERIC_PARAM)
                                        .setMultiValueMode(Search.MultiValueMode.MVM_AVG)
                                )
                                .setWeight(2)
                                .setFilter(Search.Query.newBuilder()
                                        .setType(Search.QueryType.MATCH_ALL_QUERY)
                                        .setQuery(Search.MatchAllQuery.newBuilder().build().toByteString())
                                        .build()))
                        .addFunctions(Search.Function.newBuilder()
                                .setRandom(Search.RandomScoreFunction.newBuilder()))
                        .setScoreMode(Search.FunctionScoreMode.FSM_SUM)
                        .setCombineMode(Search.FunctionCombineMode.FCM_SUM)
                        .setMinScore(10)
                        .setMaxScore(10)
                        .build().toByteString())
                .build());

        FunctionsScoreQuery expected = QueryBuilders.functionsScore()
                .query(QueryBuilders.matchAll().build())
                .addFunction(ScoreFunction.newBuilder()
                        .fieldValueFactorFunction(FieldValueFactorFunction.newBuilder()
                                .fieldName("field")
                                .factor(1)
                                .missing(1)
                                .modifier(FieldValueFactorFunction.FunctionModifier.LN1P)
                                .build())
                        .build())
                .addFunction(ScoreFunction.newBuilder()
                        .decayFunction(DecayFunction.newBuilder()
                                .fieldName("field")
                                .mathFunction(DecayFunction.MathFunction.GAUSS)
                                .decay(0.5)
                                .decayParam(DecayFuncGeoParam.newBuilder()
                                        .origin("1,1")
                                        .scale(10)
                                        .offset(0)
                                        .build())
                                .multiValueMode(MultiValueMode.AVG)
                                .build())
                        .weight(2)
                        .filter(QueryBuilders.matchAll().build())
                        .build())
                .addFunction(ScoreFunction.newBuilder()
                        .decayFunction(DecayFunction.newBuilder()
                                .fieldName("field")
                                .mathFunction(DecayFunction.MathFunction.GAUSS)
                                .decay(0.5)
                                .decayParam(DecayFuncDateParam.newBuilder()
                                        .originString("1970-01-01 00:00:00.000")
                                        .originLong(20L)
                                        .scale(new DateTimeValue(1, DateTimeUnit.MINUTE))
                                        .offset(new DateTimeValue(1, DateTimeUnit.MINUTE))
                                        .build())
                                .multiValueMode(MultiValueMode.AVG)
                                .build())
                        .weight(2)
                        .filter(QueryBuilders.matchAll().build())
                        .build())
                .addFunction(ScoreFunction.newBuilder()
                        .decayFunction(DecayFunction.newBuilder()
                                .fieldName("field")
                                .mathFunction(DecayFunction.MathFunction.GAUSS)
                                .decay(0.5)
                                .decayParam(DecayFuncNumericParam.newBuilder()
                                        .origin(2)
                                        .scale(1)
                                        .offset(0)
                                        .build())
                                .multiValueMode(MultiValueMode.AVG)
                                .build())
                        .weight(2)
                        .filter(QueryBuilders.matchAll().build())
                        .build())
                .addFunction(ScoreFunction.newBuilder()
                        .randomFunction(RandomFunction.newBuilder().build()).build())
                .scoreMode(FunctionsScoreQuery.ScoreMode.SUM)
                .combineMode(FunctionsScoreQuery.CombineMode.SUM)
                .minScore(10f)
                .maxScore(10f)
                .build();
        assertEquals(GSON.toJson(expected), GSON.toJson(actual));

    }
}
