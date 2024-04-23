package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>用于处理文档分值的Query，是{@link FunctionScoreQuery}的改进功能。</p>
 * <p>它会在查询结束后对每一个匹配的文档重新打分，并以最终分数排序。</p>
 * <p>支持{@link FieldValueFactorFunction}，{@link DecayFunction}和{@link RandomFunction}三种类型的打分方式（各种功能的示例在对应的类中解释）。
 * 同时在FunctionsScoreQuery中可以设置filter作为筛选文档条件。</p>
 */
public class FunctionsScoreQuery implements Query {
    private final QueryType queryType = QueryType.QueryType_FunctionsScoreQuery;

    /**
     * 正常的{@link Query}
     */
    private Query query;
    /**
     * function列表，每个function都包含一个打分函数，weight权重以及筛选打分条件的Filter
     */
    private List<ScoreFunction> functions;
    /**
     * 控制各个function分数的结合计算方式
     */
    private ScoreMode scoreMode;
    /**
     * 控制function分数与query分数的结合计算方式
     */
    private CombineMode combineMode;
    /**
     * 限制最小分数：低于minScore分数的文档将不会显示
     */
    private Float minScore;
    /**
     * 限制function分数结合后的最大分数，防止function分数过大
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
