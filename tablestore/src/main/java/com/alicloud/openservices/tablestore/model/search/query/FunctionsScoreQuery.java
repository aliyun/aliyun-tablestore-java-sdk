package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>A Query used for processing document scores, which is an improved feature of {@link FunctionScoreQuery}.</p>
 * <p>It re-scores each matched document after the query execution and sorts them by the final score.</p>
 * <p>It supports three types of scoring methods: {@link FieldValueFactorFunction}, {@link DecayFunction}, and {@link RandomFunction} (examples of each function are explained in their respective classes).</p>
 * <p>Additionally, a filter can be set in the FunctionsScoreQuery to serve as a document screening condition.</p>
 */
public class FunctionsScoreQuery implements Query {
    private final QueryType queryType = QueryType.QueryType_FunctionsScoreQuery;

    /**
     * Normal {@link Query}
     */
    private Query query;
    /**
     * A list of functions, each function includes a scoring function, a weight for weighting, and a Filter for filtering the scoring conditions.
     */
    private List<ScoreFunction> functions;
    /**
     * Control the combination calculation method of scores for each function
     */
    private ScoreMode scoreMode;
    /**
     * Controls the combination calculation method of function score and query score.
     */
    private CombineMode combineMode;
    /**
     * Limit the minimum score: documents with a score lower than minScore will not be displayed.
     */
    private Float minScore;
    /**
     * Limit the maximum score after combining function scores to prevent the function score from becoming too large.
     */
    private Float maxScore;

    public FunctionsScoreQuery(){}

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public List<ScoreFunction> getFunctions() {
        return functions;
    }

    public void setFunctions(List<ScoreFunction> functions) {
        this.functions = functions;
    }

    public void addFunction(ScoreFunction function) {
        if (functions == null) {
            functions = new ArrayList<ScoreFunction>();
        }
        functions.add(function);
    }

    public ScoreMode getScoreMode() {
        return scoreMode;
    }

    public void setScoreMode(ScoreMode scoreMode) {
        this.scoreMode = scoreMode;
    }

    public CombineMode getCombineMode() {
        return combineMode;
    }

    public void setCombineMode(CombineMode combineMode) {
        this.combineMode = combineMode;
    }

    public Float getMinScore() {
        return minScore;
    }

    public void setMinScore(Float minScore) {
        this.minScore = minScore;
    }

    public Float getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(Float maxScore) {
        this.maxScore = maxScore;
    }

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildFunctionsScoreQuery(this).toByteString();
    }

    protected static FunctionsScoreQuery.Builder newBuilder() {
        return new FunctionsScoreQuery.Builder();
    }

    public static final class Builder implements QueryBuilder {
        private Query query;
        private List<ScoreFunction> functions;
        private ScoreMode scoreMode;
        private CombineMode combineMode;
        private Float minScore;
        private Float maxScore;

        private Builder() {
        }

        public FunctionsScoreQuery.Builder query(QueryBuilder queryBuilder) {
            this.query = queryBuilder.build();
            return this;
        }

        public FunctionsScoreQuery.Builder functions(List<ScoreFunction> functions) {
            this.functions = functions;
            return this;
        }

        public FunctionsScoreQuery.Builder addFunction(ScoreFunction function) {
            if (functions == null) {
                functions = new ArrayList<ScoreFunction>();
            }
            functions.add(function);
            return this;
        }

        public FunctionsScoreQuery.Builder query(Query query) {
            this.query = query;
            return this;
        }

        public FunctionsScoreQuery.Builder scoreMode(ScoreMode scoreMode) {
            this.scoreMode = scoreMode;
            return this;
        }

        public FunctionsScoreQuery.Builder combineMode(CombineMode combineMode) {
            this.combineMode = combineMode;
            return this;
        }

        public FunctionsScoreQuery.Builder minScore(Float minScore) {
            this.minScore = minScore;
            return this;
        }

        public FunctionsScoreQuery.Builder maxScore(Float maxScore) {
            this.maxScore = maxScore;
            return this;
        }

        @Override
        public FunctionsScoreQuery build() {
            FunctionsScoreQuery functionsScoreQuery = new FunctionsScoreQuery();
            functionsScoreQuery.setQuery(query);
            functionsScoreQuery.setFunctions(functions);
            functionsScoreQuery.setScoreMode(scoreMode);
            functionsScoreQuery.setCombineMode(combineMode);
            functionsScoreQuery.setMinScore(minScore);
            functionsScoreQuery.setMaxScore(maxScore);
            return functionsScoreQuery;
        }
    }

    public enum ScoreMode {
        UNKNOWN,
        AVG,
        MAX,
        SUM,
        MIN,
        MULTIPLY,
        FIRST
    }

    public enum CombineMode {
        UNKNOWN,
        MULTIPLY,
        AVG,
        MAX,
        SUM,
        MIN,
        REPLACE
    }
}
