package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.model.search.query.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestScoreFunction extends BaseSearchTest {

    @Test
    public void TestScoreFunction() {
        FieldValueFactorFunction fieldValueFactorFunction = FieldValueFactorFunction.newBuilder()
                .factor(1f)
                .fieldName("a")
                .missing(1)
                .modifier(FieldValueFactorFunction.FunctionModifier.LN)
                .build();
        DecayFunction decayFunction = DecayFunction.newBuilder()
                .decay(0.5)
                .fieldName("a")
                .decayParam(DecayFuncGeoParam.newBuilder()
                        .origin(1, 1)
                        .offset(0.5)
                        .scale(1)
                        .build())
                .mathFunction(DecayFunction.MathFunction.GAUSS)
                .multiValueMode(MultiValueMode.SUM)
                .build();
        RandomFunction randomFunction = new RandomFunction();
        ScoreFunction scoreFunction = new ScoreFunction(1f,
                QueryBuilders.matchAll().build(),
                fieldValueFactorFunction,
                decayFunction,
                randomFunction);
        assertEquals(1f, scoreFunction.getWeight(), 0.01);
        assertEquals(QueryType.QueryType_MatchAllQuery, scoreFunction.getFilter().getQueryType());
        assertEquals(fieldValueFactorFunction, scoreFunction.getFieldValueFactorFunction());
        assertEquals(decayFunction, scoreFunction.getDecayFunction());
        assertEquals(randomFunction, scoreFunction.getRandomFunction());
    }

    @Test
    public void TestSetAndGetWeight() {
        ScoreFunction scoreFunction = new ScoreFunction();
        scoreFunction.setWeight(1f);
        assertEquals(1f, scoreFunction.getWeight(), 0.01);
    }

    @Test
    public void testSetAndGetFilter() {
        ScoreFunction scoreFunction = new ScoreFunction();
        scoreFunction.setFilter(QueryBuilders.matchAll().build());
        assertEquals(QueryType.QueryType_MatchAllQuery, scoreFunction.getFilter().getQueryType());
    }

    @Test
    public void TestSetAndGetFieldValueFactorFunction() {
        ScoreFunction scoreFunction = new ScoreFunction();
        FieldValueFactorFunction fieldValueFactorFunction = FieldValueFactorFunction.newBuilder()
                .factor(1f)
                .fieldName("a")
                .missing(1)
                .modifier(FieldValueFactorFunction.FunctionModifier.LN)
                .build();
        scoreFunction.setFieldValueFactorFunction(fieldValueFactorFunction);
        assertEquals(fieldValueFactorFunction, scoreFunction.getFieldValueFactorFunction());
    }

    @Test
    public void TestSetAndGetDecayFunction() {
        ScoreFunction scoreFunction = new ScoreFunction();
        DecayFunction decayFunction = DecayFunction.newBuilder()
                .decay(0.5)
                .fieldName("a")
                .decayParam(DecayFuncGeoParam.newBuilder()
                        .origin(1, 1)
                        .offset(0.5)
                        .scale(1)
                        .build())
                .mathFunction(DecayFunction.MathFunction.GAUSS)
                .multiValueMode(MultiValueMode.SUM)
                .build();
        scoreFunction.setDecayFunction(decayFunction);
        assertEquals(decayFunction, scoreFunction.getDecayFunction());
    }

    @Test
    public void TestSetAndGetRandomFunction() {
        ScoreFunction scoreFunction = new ScoreFunction();
        RandomFunction randomFunction = new RandomFunction();
        scoreFunction.setRandomFunction(randomFunction);
        assertEquals(randomFunction, scoreFunction.getRandomFunction());
    }

    @Test
    public void TestBuilderWeight() {
        ScoreFunction.Builder scoreFunction = ScoreFunction.newBuilder().weight(1f);
        assertEquals(1f, scoreFunction.weight(), 0.01);
    }

    @Test
    public void testBuilderFilter() {
        ScoreFunction.Builder scoreFunction = ScoreFunction.newBuilder().filter(QueryBuilders.matchAll().build());
        assertEquals(QueryType.QueryType_MatchAllQuery, scoreFunction.filter().getQueryType());
    }

    @Test
    public void TestBuilderFieldValueFactorFunction() {
        FieldValueFactorFunction fieldValueFactorFunction = FieldValueFactorFunction.newBuilder()
                .factor(1f)
                .fieldName("a")
                .missing(1)
                .modifier(FieldValueFactorFunction.FunctionModifier.LN)
                .build();
        ScoreFunction.Builder scoreFunction = ScoreFunction.newBuilder().fieldValueFactorFunction(fieldValueFactorFunction);
        assertEquals(fieldValueFactorFunction, scoreFunction.fieldValueFactorFunction());
    }

    @Test
    public void TestBuilderDecayFunction() {
        DecayFunction decayFunction = DecayFunction.newBuilder()
                .decay(0.5)
                .fieldName("a")
                .decayParam(DecayFuncGeoParam.newBuilder()
                        .origin(1, 1)
                        .offset(0.5)
                        .scale(1)
                        .build())
                .mathFunction(DecayFunction.MathFunction.GAUSS)
                .multiValueMode(MultiValueMode.SUM)
                .build();
        ScoreFunction.Builder scoreFunction = ScoreFunction.newBuilder().decayFunction(decayFunction);
        assertEquals(decayFunction, scoreFunction.decayFunction());
    }

    @Test
    public void TestBuilderRandomFunction() {
        RandomFunction randomFunction = new RandomFunction();
        ScoreFunction.Builder scoreFunction = ScoreFunction.newBuilder().randomFunction(randomFunction);
        assertEquals(randomFunction, scoreFunction.randomFunction());
    }
}
