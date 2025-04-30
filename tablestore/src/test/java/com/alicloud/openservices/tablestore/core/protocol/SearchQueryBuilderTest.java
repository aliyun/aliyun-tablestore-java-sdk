package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.query.*;

import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import static com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder.buildFunctionsScoreQuery;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SearchQueryBuilderTest extends BaseSearchTest {
    // exists query
    @Test
    public void testExistsQuery() {
        ExistsQuery query = new ExistsQuery();
        query.setFieldName("FieldName");

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);
        assertEquals(Search.QueryType.EXISTS_QUERY, queryPB.getType());

        Search.ExistsQuery.Builder builder = Search.ExistsQuery.newBuilder();
        builder.setFieldName("FieldName");
        assertEquals(builder.build().toByteString(), queryPB.getQuery());
    }

    @Test
    public void testExistsQueryEmptyFieldName() {
        ExistsQuery query = new ExistsQuery();

        try {
            SearchQueryBuilder.buildQuery(query);
            fail();
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testEmptyQuery() {
        SearchQuery searchQuery = new SearchQuery();

        try {
            SearchQueryBuilder.buildQuery(searchQuery.getQuery());
            fail();
        } catch (NullPointerException ignored) {
        }
    }

    // match phrase query
    @Test
    public void testMatchPhraseQuery() {
        MatchPhraseQuery query = new MatchPhraseQuery();
        query.setFieldName("FieldName");
        query.setText("FieldValue");
        query.setWeight(2.0f);

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);
        assertEquals(Search.QueryType.MATCH_PHRASE_QUERY, queryPB.getType());

        Search.MatchPhraseQuery.Builder builder = Search.MatchPhraseQuery.newBuilder();
        builder.setFieldName("FieldName");
        builder.setText("FieldValue");
        builder.setWeight(2.0f);
        assertEquals(builder.build().toByteString(), queryPB.getQuery());
    }

    @Test
    public void testMatchPhraseQueryEmptyFieldName() {
        MatchPhraseQuery query = new MatchPhraseQuery();

        try {
            SearchQueryBuilder.buildQuery(query);
            fail();
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testMatchPhraseQueryEmptyWeight() {
        MatchPhraseQuery query = new MatchPhraseQuery();
        query.setFieldName("FieldName");
        query.setText("FieldValue");

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);
        Search.MatchPhraseQuery.Builder builder = Search.MatchPhraseQuery.newBuilder();
        builder.setFieldName("FieldName");
        builder.setText("FieldValue");
        builder.setWeight(1.0f);  // weight is 1.0 by default
        assertEquals(builder.build().toByteString(), queryPB.getQuery());

    }

    // nested query
    @Test
    public void testNestedQuery() {
        TermQuery termQuery = new TermQuery();
        termQuery.setFieldName("user.first_name");
        termQuery.setTerm(ColumnValue.fromString("Samuel"));

        NestedQuery query = new NestedQuery();
        query.setPath("user");
        query.setQuery(termQuery);
        query.setScoreMode(ScoreMode.None);
        query.setWeight(2.0f);

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);

        // assert
        Search.Query.Builder builder = Search.Query.newBuilder();
        builder.setQuery(query.serialize());
        builder.setType(Search.QueryType.NESTED_QUERY);

        assertEquals(builder.build().getQuery(), queryPB.getQuery());
        assertEquals(Search.QueryType.NESTED_QUERY, queryPB.getType());
    }

    @Test
    public void testNestedQueryEmptyPath() {
        TermQuery termQuery = new TermQuery();
        termQuery.setFieldName("user.first_name");
        termQuery.setTerm(ColumnValue.fromString("Samuel"));

        NestedQuery query = new NestedQuery();
        //query.setPath("user");
        query.setQuery(termQuery);
        query.setScoreMode(ScoreMode.None);
        query.setWeight(2.0f);

        try {
            SearchQueryBuilder.buildQuery(query);
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testNestedQueryEmptyTermQuery() {
        TermQuery termQuery = new TermQuery();
        termQuery.setFieldName("user.first_name");
        termQuery.setTerm(ColumnValue.fromString("Samuel"));

        NestedQuery query = new NestedQuery();
        query.setPath("user");
        //query.setQuery(termQuery);
        query.setScoreMode(ScoreMode.None);
        query.setWeight(2.0f);

        try {
            SearchQueryBuilder.buildQuery(query);
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testNestedQueryEmptyScoreMode() {
        TermQuery termQuery = new TermQuery();
        termQuery.setFieldName("user.first_name");
        termQuery.setTerm(ColumnValue.fromString("Samuel"));

        NestedQuery query = new NestedQuery();
        query.setPath("user");
        query.setQuery(termQuery);
        //query.setScoreMode(ScoreMode.None);
        query.setWeight(2.0f);

        try {
            SearchQueryBuilder.buildQuery(query);
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testNestedQueryEmptyWeight() {
        TermQuery termQuery = new TermQuery();
        termQuery.setFieldName("user.first_name");
        termQuery.setTerm(ColumnValue.fromString("Samuel"));

        NestedQuery query = new NestedQuery();
        query.setPath("user");
        query.setQuery(termQuery);
        query.setScoreMode(ScoreMode.None);
        //query.setWeight(2.0f);

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);

        // assert
        NestedQuery query2 = new NestedQuery();
        query2.setPath("user");
        query2.setQuery(termQuery);
        query2.setScoreMode(ScoreMode.None);
        query2.setWeight(1.0f);   // weight is 1.0 by default

        Search.Query.Builder builder = Search.Query.newBuilder();
        builder.setQuery(query2.serialize());
        builder.setType(Search.QueryType.NESTED_QUERY);

        assertEquals(builder.build().getQuery(), queryPB.getQuery());
        assertEquals(Search.QueryType.NESTED_QUERY, queryPB.getType());
    }

    // prefix query
    @Test
    public void testPrefixQuery() {
        PrefixQuery query = new PrefixQuery();
        query.setFieldName("FieldName");
        query.setPrefix("FieldValue");
        query.setWeight(2.0f);

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);
        assertEquals(Search.QueryType.PREFIX_QUERY, queryPB.getType());

        Search.PrefixQuery.Builder builder = Search.PrefixQuery.newBuilder();
        builder.setFieldName("FieldName");
        builder.setPrefix("FieldValue");
        builder.setWeight(2.0f);
        assertEquals(builder.build().toByteString(), queryPB.getQuery());
    }

    @Test
    public void testPrefixQueryEmptyFieldName() {
        PrefixQuery query = new PrefixQuery();
        //query.setFieldName("FieldName");
        query.setPrefix("FieldValue");
        query.setWeight(2.0f);

        try {
            SearchQueryBuilder.buildQuery(query);
            fail();
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testPrefixQueryEmptyFieldValue() {
        PrefixQuery query = new PrefixQuery();
        query.setFieldName("FieldName");
        //query.setPrefix("FieldValue");
        query.setWeight(2.0f);

        try {
            SearchQueryBuilder.buildQuery(query);
            fail();
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testPrefixQueryEmptyWeight() {
        PrefixQuery query = new PrefixQuery();
        query.setFieldName("FieldName");
        query.setPrefix("FieldValue");
        //query.setWeight(2.0f);

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);
        assertEquals(Search.QueryType.PREFIX_QUERY, queryPB.getType());

        Search.PrefixQuery.Builder builder = Search.PrefixQuery.newBuilder();
        builder.setFieldName("FieldName");
        builder.setPrefix("FieldValue");
        builder.setWeight(1.0f);  // weight is 1.0 by default
        assertEquals(builder.build().toByteString(), queryPB.getQuery());
    }

    @Test
    public void testSuffixQuery() {
        SuffixQuery query = new SuffixQuery();
        query.setFieldName("FieldName");
        query.setSuffix("FieldValue");
        query.setWeight(2.0f);

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);
        assertEquals(Search.QueryType.SUFFIX_QUERY, queryPB.getType());

        Search.SuffixQuery.Builder builder = Search.SuffixQuery.newBuilder();
        builder.setFieldName("FieldName");
        builder.setSuffix("FieldValue");
        builder.setWeight(2.0f);
        assertEquals(builder.build().toByteString(), queryPB.getQuery());
    }

    @Test
    public void testSuffixQueryEmptyFieldName() {
        SuffixQuery query = new SuffixQuery();
        //query.setFieldName("FieldName");
        query.setSuffix("FieldValue");
        query.setWeight(2.0f);

        try {
            SearchQueryBuilder.buildQuery(query);
            fail();
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testSuffixQueryEmptyFieldValue() {
        SuffixQuery query = new SuffixQuery();
        query.setFieldName("FieldName");
        //query.setSuffix("FieldValue");
        query.setWeight(2.0f);

        try {
            SearchQueryBuilder.buildQuery(query);
            fail();
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testSuffixQueryEmptyWeight() {
        SuffixQuery query = new SuffixQuery();
        query.setFieldName("FieldName");
        query.setSuffix("FieldValue");
        //query.setWeight(2.0f);

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);
        assertEquals(Search.QueryType.SUFFIX_QUERY, queryPB.getType());

        Search.SuffixQuery.Builder builder = Search.SuffixQuery.newBuilder();
        builder.setFieldName("FieldName");
        builder.setSuffix("FieldValue");
        builder.setWeight(1.0f);  // weight is 1.0 by default
        assertEquals(builder.build().toByteString(), queryPB.getQuery());
    }

    // term query
    @Test
    public void testTermQuery() {
        TermQuery query = new TermQuery();
        query.setFieldName("FieldName");
        query.setTerm(ColumnValue.fromString("FieldValue"));
        query.setWeight(2.0f);

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);
        assertEquals(Search.QueryType.TERM_QUERY, queryPB.getType());

        Search.TermQuery.Builder builder = Search.TermQuery.newBuilder();
        builder.setFieldName("FieldName");
        builder.setTerm(ByteString.copyFrom(SearchVariantType.toVariant(ColumnValue.fromString("FieldValue"))));
        builder.setWeight(2.0f);
        assertEquals(builder.build().toByteString(), queryPB.getQuery());
    }

    @Test
    public void testTermQueryEmptyFieldName() {
        TermQuery query = new TermQuery();
        //query.setFieldName("FieldName");
        query.setTerm(ColumnValue.fromString("FieldValue"));
        query.setWeight(2.0f);

        try {
            SearchQueryBuilder.buildQuery(query);
            fail();
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testTermQueryEmptyTerm() {
        TermQuery query = new TermQuery();
        query.setFieldName("FieldName");
        //query.setTerm(ColumnValue.fromString("FieldValue"));
        query.setWeight(2.0f);

        try {
            SearchQueryBuilder.buildQuery(query);
            fail();
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testTermQueryEmptyWeight() {
        TermQuery query = new TermQuery();
        query.setFieldName("FieldName");
        query.setTerm(ColumnValue.fromString("FieldValue"));
        //query.setWeight(2.0f);

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);
        assertEquals(Search.QueryType.TERM_QUERY, queryPB.getType());

        Search.TermQuery.Builder builder = Search.TermQuery.newBuilder();
        builder.setFieldName("FieldName");
        builder.setTerm(ByteString.copyFrom(SearchVariantType.toVariant(ColumnValue.fromString("FieldValue"))));
        builder.setWeight(1.0f);  // weight is 1.0 by default
        assertEquals(builder.build().toByteString(), queryPB.getQuery());
    }

    // terms query
    @Test
    public void testTermsQuery() {
        TermsQuery query = new TermsQuery();
        query.setFieldName("FieldName");
        query.setTerms(Arrays.asList(ColumnValue.fromString("FieldValue1"), ColumnValue.fromString("FieldValue2")));
        query.setWeight(2.0f);

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);
        assertEquals(Search.QueryType.TERMS_QUERY, queryPB.getType());

        Search.TermsQuery.Builder builder = Search.TermsQuery.newBuilder();
        builder.setFieldName("FieldName");
        builder.addTerms(ByteString.copyFrom(SearchVariantType.toVariant(ColumnValue.fromString("FieldValue1"))));
        builder.addTerms(ByteString.copyFrom(SearchVariantType.toVariant(ColumnValue.fromString("FieldValue2"))));
        builder.setWeight(2.0f);
        assertEquals(builder.build().toByteString(), queryPB.getQuery());
    }

    @Test
    public void testTermsQueryEmptyFieldName() {
        TermsQuery query = new TermsQuery();
        //query.setFieldName("FieldName");
        query.setTerms(Arrays.asList(ColumnValue.fromString("FieldValue1"), ColumnValue.fromString("FieldValue2")));
        query.setWeight(2.0f);

        try {
            SearchQueryBuilder.buildQuery(query);
            fail();
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testTermsQueryEmptyTerms() {
        TermsQuery query = new TermsQuery();
        query.setFieldName("FieldName");
        //query.setTerms(Arrays.asList(ColumnValue.fromString("FieldValue1"), ColumnValue.fromString("FieldValue2")));
        query.setWeight(2.0f);

        try {
            SearchQueryBuilder.buildQuery(query);
            fail();
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testTermsQueryEmptyWeight() {
        TermsQuery query = new TermsQuery();
        query.setFieldName("FieldName");
        query.setTerms(Arrays.asList(ColumnValue.fromString("FieldValue1"), ColumnValue.fromString("FieldValue2")));
        //query.setWeight(2.0f);

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);
        assertEquals(Search.QueryType.TERMS_QUERY, queryPB.getType());

        Search.TermsQuery.Builder builder = Search.TermsQuery.newBuilder();
        builder.setFieldName("FieldName");
        builder.addTerms(ByteString.copyFrom(SearchVariantType.toVariant(ColumnValue.fromString("FieldValue1"))));
        builder.addTerms(ByteString.copyFrom(SearchVariantType.toVariant(ColumnValue.fromString("FieldValue2"))));
        builder.setWeight(1.0f);  // weight is 1.0 by default
        assertEquals(builder.build().toByteString(), queryPB.getQuery());
    }

    // term query
    @Test
    public void testWildcardQuery() {
        WildcardQuery query = new WildcardQuery();
        query.setFieldName("FieldName");
        query.setValue("FieldValue");
        query.setWeight(2.0f);

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);
        assertEquals(Search.QueryType.WILDCARD_QUERY, queryPB.getType());

        Search.WildcardQuery.Builder builder = Search.WildcardQuery.newBuilder();
        builder.setFieldName("FieldName");
        builder.setValue("FieldValue");
        builder.setWeight(2.0f);
        assertEquals(builder.build().toByteString(), queryPB.getQuery());
    }

    @Test
    public void testWildcardQueryEmptyFieldName() {
        WildcardQuery query = new WildcardQuery();
        //query.setFieldName("FieldName");
        query.setValue("FieldValue");
        query.setWeight(2.0f);

        try {
            SearchQueryBuilder.buildQuery(query);
            fail();
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testWildcardQueryEmptyValue() {
        WildcardQuery query = new WildcardQuery();
        query.setFieldName("FieldName");
        //query.setValue("FieldValue");
        query.setWeight(2.0f);

        try {
            SearchQueryBuilder.buildQuery(query);
            fail();
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testWildcardQueryEmptyWeight() {
        WildcardQuery query = new WildcardQuery();
        query.setFieldName("FieldName");
        query.setValue("FieldValue");
        //query.setWeight(2.0f);

        Search.Query queryPB = SearchQueryBuilder.buildQuery(query);
        assertEquals(Search.QueryType.WILDCARD_QUERY, queryPB.getType());

        Search.WildcardQuery.Builder builder = Search.WildcardQuery.newBuilder();
        builder.setFieldName("FieldName");
        builder.setValue("FieldValue");
        builder.setWeight(1.0f);  // weight is 1.0 by default
        assertEquals(builder.build().toByteString(), queryPB.getQuery());
    }

    @Test
    public void testBuildModifier() {
        assertEquals(11, FieldValueFactorFunction.FunctionModifier.values().length);
        assertEquals(10, Search.FunctionModifier.values().length);
        FieldValueFactorFunction.FunctionModifier[] modifier = new FieldValueFactorFunction.FunctionModifier[]{
                FieldValueFactorFunction.FunctionModifier.NONE,
                FieldValueFactorFunction.FunctionModifier.LOG,
                FieldValueFactorFunction.FunctionModifier.LOG1P,
                FieldValueFactorFunction.FunctionModifier.LOG2P,
                FieldValueFactorFunction.FunctionModifier.LN,
                FieldValueFactorFunction.FunctionModifier.LN1P,
                FieldValueFactorFunction.FunctionModifier.LN2P,
                FieldValueFactorFunction.FunctionModifier.SQUARE,
                FieldValueFactorFunction.FunctionModifier.SQRT,
                FieldValueFactorFunction.FunctionModifier.RECIPROCAL
        };
        Search.FunctionModifier[] PbModifier = new Search.FunctionModifier[]{
                Search.FunctionModifier.FM_NONE,
                Search.FunctionModifier.FM_LOG,
                Search.FunctionModifier.FM_LOG1P,
                Search.FunctionModifier.FM_LOG2P,
                Search.FunctionModifier.FM_LN,
                Search.FunctionModifier.FM_LN1P,
                Search.FunctionModifier.FM_LN2P,
                Search.FunctionModifier.FM_SQUARE,
                Search.FunctionModifier.FM_SQRT,
                Search.FunctionModifier.FM_RECIPROCAL
        };
        for (int i = 0; i < Search.FunctionModifier.values().length; i++) {
            FieldValueFactorFunction fieldValueFactorFunction = new FieldValueFactorFunction(null, null, modifier[i], null);
            assertEquals(PbModifier[i], buildFunctionsScoreQuery(QueryBuilders.functionsScore()
                    .addFunction(ScoreFunction.newBuilder()
                            .fieldValueFactorFunction(fieldValueFactorFunction)
                            .build())
                    .build())
                    .getFunctions(0)
                    .getFieldValueFactor()
                    .getModifier());
        }
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testBuildModifierUnknownModifier() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("unknown modifier: UNKNOWN");

        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore()
                .addFunction(ScoreFunction.newBuilder()
                        .fieldValueFactorFunction(new FieldValueFactorFunction(null, null, FieldValueFactorFunction.FunctionModifier.UNKNOWN, null))
                        .build())
                .build();
        buildFunctionsScoreQuery(functionsScoreQuery);
    }

    @Test
    public void testBuildDecayMathFunction() {
        assertEquals(4, DecayFunction.MathFunction.values().length);
        assertEquals(3, Search.DecayMathFunction.values().length);
        DecayFunction.MathFunction[] mathFunction = new DecayFunction.MathFunction[]{
                DecayFunction.MathFunction.GAUSS,
                DecayFunction.MathFunction.EXP,
                DecayFunction.MathFunction.LINEAR,
                DecayFunction.MathFunction.UNKNOWN
        };
        Search.DecayMathFunction[] PbMathFunction = new Search.DecayMathFunction[]{
                Search.DecayMathFunction.GAUSS,
                Search.DecayMathFunction.EXP,
                Search.DecayMathFunction.LINEAR
        };
        for (int i = 0; i < Search.DecayMathFunction.values().length; i++) {
            DecayFunction decayFunction = DecayFunction.newBuilder().mathFunction(mathFunction[i]).decayParam(DecayFuncGeoParam.newBuilder().build()).build();
            assertEquals(PbMathFunction[i], buildFunctionsScoreQuery(QueryBuilders.functionsScore()
                    .addFunction(ScoreFunction.newBuilder()
                            .decayFunction(decayFunction)
                            .build())
                    .build())
                    .getFunctions(0)
                    .getDecay()
                    .getMathFunction());
        }
    }

    @Test
    public void testBuildDecayMathFunctionUnknownMathFunction() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("unknown MathFunction: UNKNOWN");

        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore()
                .addFunction(ScoreFunction.newBuilder()
                        .decayFunction(new DecayFunction(null, DecayFuncGeoParam.newBuilder().build(), DecayFunction.MathFunction.UNKNOWN, null, null))
                        .build())
                .build();
        buildFunctionsScoreQuery(functionsScoreQuery);
    }

    @Test
    public void testBuildMultiValueMode() {
        assertEquals(5, MultiValueMode.values().length);
        assertEquals(4, Search.MultiValueMode.values().length);
        MultiValueMode[] multiValueMode = new MultiValueMode[]{
                MultiValueMode.MIN,
                MultiValueMode.MAX,
                MultiValueMode.AVG,
                MultiValueMode.SUM,
        };
        Search.MultiValueMode[] PbMultiValueMode = new Search.MultiValueMode[]{
                Search.MultiValueMode.MVM_MIN,
                Search.MultiValueMode.MVM_MAX,
                Search.MultiValueMode.MVM_AVG,
                Search.MultiValueMode.MVM_SUM
        };
        for (int i = 0; i < Search.MultiValueMode.values().length; i++) {
            DecayFunction decayFunction = DecayFunction.newBuilder().multiValueMode(multiValueMode[i]).decayParam(DecayFuncGeoParam.newBuilder().build()).build();
            assertEquals(PbMultiValueMode[i], buildFunctionsScoreQuery(QueryBuilders.functionsScore()
                    .addFunction(ScoreFunction.newBuilder()
                            .decayFunction(decayFunction)
                            .build())
                    .build())
                    .getFunctions(0)
                    .getDecay()
                    .getMultiValueMode());
        }
    }

    @Test
    public void testBuildMultiValueModeUnknownMultiValueMode() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("unknown MultiValueMode: UNKNOWN");

        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore()
                .addFunction(ScoreFunction.newBuilder()
                        .decayFunction(new DecayFunction(null, DecayFuncGeoParam.newBuilder().build(), DecayFunction.MathFunction.EXP, null, MultiValueMode.UNKNOWN))
                        .build())
                .build();
        buildFunctionsScoreQuery(functionsScoreQuery);
    }

    @Test
    public void testBuildFunctionScoreMode() {
        assertEquals(7, FunctionsScoreQuery.ScoreMode.values().length);
        assertEquals(6, Search.FunctionScoreMode.values().length);
        FunctionsScoreQuery.ScoreMode[] scoreMode = new FunctionsScoreQuery.ScoreMode[]{
                FunctionsScoreQuery.ScoreMode.AVG,
                FunctionsScoreQuery.ScoreMode.MAX,
                FunctionsScoreQuery.ScoreMode.SUM,
                FunctionsScoreQuery.ScoreMode.MIN,
                FunctionsScoreQuery.ScoreMode.MULTIPLY,
                FunctionsScoreQuery.ScoreMode.FIRST,
        };
        Search.FunctionScoreMode[] PbScoreMode = new Search.FunctionScoreMode[]{
                Search.FunctionScoreMode.FSM_AVG,
                Search.FunctionScoreMode.FSM_MAX,
                Search.FunctionScoreMode.FSM_SUM,
                Search.FunctionScoreMode.FSM_MIN,
                Search.FunctionScoreMode.FSM_MULTIPLY,
                Search.FunctionScoreMode.FSM_FIRST
        };
        for (int i = 0; i < Search.FunctionScoreMode.values().length; i++) {
            FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore()
                    .scoreMode(scoreMode[i])
                    .build();
            assertEquals(PbScoreMode[i], buildFunctionsScoreQuery(functionsScoreQuery).getScoreMode());
        }
    }

    @Test
    public void testBuildFunctionScoreModeUnknownScoreMode() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("unknown FunctionsScoreQuery.ScoreMode: UNKNOWN");

        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore()
                .scoreMode(FunctionsScoreQuery.ScoreMode.UNKNOWN)
                .build();
        buildFunctionsScoreQuery(functionsScoreQuery);
    }

    @Test
    public void testBuildCombineMode() {
        assertEquals(7, FunctionsScoreQuery.CombineMode.values().length);
        assertEquals(6, Search.FunctionCombineMode.values().length);
        FunctionsScoreQuery.CombineMode[] combineMode = new FunctionsScoreQuery.CombineMode[]{
                FunctionsScoreQuery.CombineMode.MULTIPLY,
                FunctionsScoreQuery.CombineMode.AVG,
                FunctionsScoreQuery.CombineMode.MAX,
                FunctionsScoreQuery.CombineMode.SUM,
                FunctionsScoreQuery.CombineMode.MIN,
                FunctionsScoreQuery.CombineMode.REPLACE
        };
        Search.FunctionCombineMode[] PbCombineMode = new Search.FunctionCombineMode[]{
                Search.FunctionCombineMode.FCM_MULTIPLY,
                Search.FunctionCombineMode.FCM_AVG,
                Search.FunctionCombineMode.FCM_MAX,
                Search.FunctionCombineMode.FCM_SUM,
                Search.FunctionCombineMode.FCM_MIN,
                Search.FunctionCombineMode.FCM_REPLACE
        };
        for (int i = 0; i < Search.FunctionCombineMode.values().length; i++) {
            FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore()
                    .combineMode(combineMode[i])
                    .build();
            assertEquals(PbCombineMode[i], buildFunctionsScoreQuery(functionsScoreQuery).getCombineMode());
        }
    }

    @Test
    public void testBuildCombineModeUnknownCombineMode() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("unknown FunctionsScoreQuery.CombineMode: UNKNOWN");

        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore()
                .combineMode(FunctionsScoreQuery.CombineMode.UNKNOWN)
                .build();
        buildFunctionsScoreQuery(functionsScoreQuery);
    }

    @Test
    public void testBuildFunctionsScoreQueryWithoutDecayParam() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("decayParam is empty");

        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore()
                .addFunction(ScoreFunction.newBuilder()
                        .decayFunction(DecayFunction.newBuilder()
                                .build())
                        .build())
                .build();
        buildFunctionsScoreQuery(functionsScoreQuery);
    }

    @Test
    public void testBuildFunctionsScoreQueryWithUnknownDecayParamType() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("unknown decayParamType: UNKNOWN");

        FunctionsScoreQuery functionsScoreQuery = QueryBuilders.functionsScore()
                .addFunction(ScoreFunction.newBuilder()
                        .decayFunction(DecayFunction.newBuilder()
                                .decayParam(DecayParam.unknownTypeParam())
                                .build())
                        .build())
                .build();
        buildFunctionsScoreQuery(functionsScoreQuery);
    }
}
