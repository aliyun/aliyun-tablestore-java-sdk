package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.aliyun.ots.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder.buildFunctionsScoreQuery;
import static org.junit.Assert.assertEquals;

public class TestFunctionsScoreQuery extends BaseSearchTest {
    @Test
    public void testSetAndGetQuery() {
        FunctionsScoreQuery functionsScoreQuery = new FunctionsScoreQuery();
        functionsScoreQuery.setQuery(QueryBuilders.matchAll().build());
        assertEquals(QueryType.QueryType_MatchAllQuery, functionsScoreQuery.getQuery().getQueryType());
    }

    @Test
    public void testSetAndGetFunctions() {
        ScoreFunction scoreFunction1 = ScoreFunction.newBuilder()
                .fieldValueFactorFunction(FieldValueFactorFunction.newBuilder()
                        .fieldName("col_double")
                        .factor(1.0f)
                        .missing(1000.0)
                        .modifier(FieldValueFactorFunction.FunctionModifier.NONE).build())
                .weight(1.0f)
                .filter(QueryBuilders.matchAll().build()).build();
        ScoreFunction scoreFunction2 = ScoreFunction.newBuilder()
                .randomFunction(RandomFunction.newBuilder().build()).build();
        List<ScoreFunction> scoreFunctions = Arrays.asList(scoreFunction1, scoreFunction2);
        FunctionsScoreQuery functionsScoreQuery = new FunctionsScoreQuery();
        functionsScoreQuery.setFunctions(scoreFunctions);
        assertEquals(scoreFunctions, functionsScoreQuery.getFunctions());
    }

    @Test
    public void testAddFunctions() {
        ScoreFunction scoreFunction1 = ScoreFunction.newBuilder()
                .fieldValueFactorFunction(FieldValueFactorFunction.newBuilder()
                        .fieldName("col_double")
                        .factor(1.0f)
                        .missing(1000.0)
                        .modifier(FieldValueFactorFunction.FunctionModifier.NONE).build())
                .weight(1.0f)
                .filter(QueryBuilders.matchAll().build()).build();
        ScoreFunction scoreFunction2 = ScoreFunction.newBuilder()
                .randomFunction(RandomFunction.newBuilder().build()).build();
        FunctionsScoreQuery functionsScoreQuery = new FunctionsScoreQuery();
        functionsScoreQuery.addFunction(scoreFunction1);
        functionsScoreQuery.addFunction(scoreFunction2);
        List<ScoreFunction> scoreFunctions = Arrays.asList(scoreFunction1, scoreFunction2);
        assertEquals(scoreFunctions, functionsScoreQuery.getFunctions());
    }

    @Test
    public void testSetAndGetScoreMode() {
        FunctionsScoreQuery functionsScoreQuery = new FunctionsScoreQuery();
        functionsScoreQuery.setScoreMode(FunctionsScoreQuery.ScoreMode.MULTIPLY);
        assertEquals(FunctionsScoreQuery.ScoreMode.MULTIPLY, functionsScoreQuery.getScoreMode());
    }

    @Test
    public void testSetAndGetCombineMode() {
        FunctionsScoreQuery functionsScoreQuery = new FunctionsScoreQuery();
        functionsScoreQuery.setCombineMode(FunctionsScoreQuery.CombineMode.MULTIPLY);
        assertEquals(FunctionsScoreQuery.CombineMode.MULTIPLY, functionsScoreQuery.getCombineMode());
    }

    @Test
    public void testSetAndGetMinScore() {
        FunctionsScoreQuery functionsScoreQuery = new FunctionsScoreQuery();
        functionsScoreQuery.setMinScore(1f);
        assertEquals(1f, functionsScoreQuery.getMinScore(), 0.01);
    }

    @Test
    public void testSetAndGetMaxScore() {
        FunctionsScoreQuery functionsScoreQuery = new FunctionsScoreQuery();
        functionsScoreQuery.setMaxScore(1f);
        assertEquals(1f, functionsScoreQuery.getMaxScore(), 0.01);
    }

    @Test
    public void testSerializeAddNewBuilder() throws InvalidProtocolBufferException {
        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore()
                .query(QueryBuilders.matchAll())
                .addFunction(ScoreFunction.newBuilder()
                        .fieldValueFactorFunction(FieldValueFactorFunction.newBuilder()
                                .fieldName("col_double")
                                .factor(1.0f)
                                .missing(1000.0)
                                .modifier(FieldValueFactorFunction.FunctionModifier.NONE).build())
                        .weight(1.0f)
                        .filter(QueryBuilders.matchAll().build()).build())
                .addFunction(ScoreFunction.newBuilder()
                        .randomFunction(RandomFunction.newBuilder().build()).build())
                .addFunction(ScoreFunction.newBuilder()
                        .decayFunction(DecayFunction.newBuilder()
                                .mathFunction(DecayFunction.MathFunction.GAUSS)
                                .decay(0.5)
                                .decayParam(DecayFuncGeoParam.newBuilder()
                                        .origin(1, 1)
                                        .offset(0.2)
                                        .scale(0.1)
                                        .build())
                                .build())
                        .build())
                .scoreMode(FunctionsScoreQuery.ScoreMode.MULTIPLY)
                .combineMode(FunctionsScoreQuery.CombineMode.MULTIPLY)
                .minScore(10f)
                .maxScore(20f)
                .build();
        Search.FunctionsScoreQuery pbQuery = Search.FunctionsScoreQuery.newBuilder()
                .setQuery(Search.Query.newBuilder().setQuery(Search.MatchAllQuery.newBuilder().build().toByteString()).setType(Search.QueryType.MATCH_ALL_QUERY))
                .addFunctions(Search.Function.newBuilder()
                        .setFieldValueFactor(Search.FieldValueFactorFunction.newBuilder()
                                .setFieldName("col_double")
                                .setFactor(1f)
                                .setMissing(1000d)
                                .setModifier(Search.FunctionModifier.FM_NONE).build())
                        .setWeight(1f)
                        .setFilter(Search.Query.newBuilder().setQuery(Search.MatchAllQuery.newBuilder().build().toByteString()).setType(Search.QueryType.MATCH_ALL_QUERY)).build())
                .addFunctions(Search.Function.newBuilder()
                        .setRandom(Search.RandomScoreFunction.newBuilder().build()).build())
                .addFunctions(Search.Function.newBuilder()
                        .setDecay(Search.DecayFunction.newBuilder()
                                .setMathFunction(Search.DecayMathFunction.GAUSS)
                                .setDecay(0.5)
                                .setParamType(Search.DecayFuncParamType.DF_GEO_PARAM)
                                .setParam(Search.DecayFuncGeoParam.newBuilder()
                                        .setOrigin("1.0,1.0")
                                        .setOffset(0.2)
                                        .setScale(0.1)
                                        .build().toByteString())
                                .build())
                        .build())
                .setScoreMode(Search.FunctionScoreMode.FSM_MULTIPLY)
                .setCombineMode(Search.FunctionCombineMode.FCM_MULTIPLY)
                .setMinScore(10f)
                .setMaxScore(20f)
                .build();
        assertEquals(pbQuery, buildFunctionsScoreQuery(functionsScoreQuery));
    }

    @Test
    public void testBuilderQuery_1() {
        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore().query(QueryBuilders.matchAll().build()).build();
        assertEquals(QueryType.QueryType_MatchAllQuery, functionsScoreQuery.getQuery().getQueryType());
    }

    @Test
    public void testBuilderQuery_2() {
        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore().query(QueryBuilders.matchAll()).build();
        assertEquals(QueryType.QueryType_MatchAllQuery, functionsScoreQuery.getQuery().getQueryType());
    }

    @Test
    public void testBuilderFunctions() {
        ScoreFunction scoreFunction1 = ScoreFunction.newBuilder()
                .fieldValueFactorFunction(FieldValueFactorFunction.newBuilder()
                        .fieldName("col_double")
                        .factor(1.0f)
                        .missing(1000.0)
                        .modifier(FieldValueFactorFunction.FunctionModifier.NONE).build())
                .weight(1.0f)
                .filter(QueryBuilders.matchAll().build()).build();
        ScoreFunction scoreFunction2 = ScoreFunction.newBuilder()
                .randomFunction(RandomFunction.newBuilder().build()).build();
        List<ScoreFunction> scoreFunctions = Arrays.asList(scoreFunction1, scoreFunction2);
        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore().functions(scoreFunctions).build();
        assertEquals(scoreFunctions, functionsScoreQuery.getFunctions());
    }

    @Test
    public void testBuilderAddFunctions() {
        ScoreFunction scoreFunction1 = ScoreFunction.newBuilder()
                .fieldValueFactorFunction(FieldValueFactorFunction.newBuilder()
                        .fieldName("col_double")
                        .factor(1.0f)
                        .missing(1000.0)
                        .modifier(FieldValueFactorFunction.FunctionModifier.NONE).build())
                .weight(1.0f)
                .filter(QueryBuilders.matchAll().build()).build();
        ScoreFunction scoreFunction2 = ScoreFunction.newBuilder()
                .randomFunction(RandomFunction.newBuilder().build()).build();
        List<ScoreFunction> scoreFunctions = Arrays.asList(scoreFunction1, scoreFunction2);
        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore().addFunction(scoreFunction1).addFunction(scoreFunction2).build();
        assertEquals(scoreFunctions, functionsScoreQuery.getFunctions());
    }

    @Test
    public void testBuilderScoreMode() {
        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore().scoreMode(FunctionsScoreQuery.ScoreMode.MULTIPLY).build();
        assertEquals(FunctionsScoreQuery.ScoreMode.MULTIPLY, functionsScoreQuery.getScoreMode());
    }

    @Test
    public void testBuilderCombineMode() {
        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore().combineMode(FunctionsScoreQuery.CombineMode.MULTIPLY).build();
        assertEquals(FunctionsScoreQuery.CombineMode.MULTIPLY, functionsScoreQuery.getCombineMode());
    }

    @Test
    public void testBuilderMinScore() {
        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore().minScore(1f).build();
        assertEquals(1f, functionsScoreQuery.getMinScore(), 0.01);
    }

    @Test
    public void testBuilderMaxScore() {
        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore().maxScore(1f).build();
        assertEquals(1f, functionsScoreQuery.getMaxScore(), 0.01);
    }
}
