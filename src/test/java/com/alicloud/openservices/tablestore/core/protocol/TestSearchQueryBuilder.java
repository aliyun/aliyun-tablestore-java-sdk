package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.query.*;

import com.google.protobuf.ByteString;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestSearchQueryBuilder extends BaseSearchTest {
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
}
