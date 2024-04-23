package com.alicloud.openservices.tablestore.model.search.query;

/**
 * <p>每个ScoreFunction都是一个打分函数，目前最多支持三个ScoreFunction同时打分。</p>
 * <p>每个ScoreFunction中都包含{@link FieldValueFactorFunction}、{@link DecayFunction}和{@link RandomFunction}三种函数，请选择其中一种设置或均不设置（只使用filter和weight）。</p>
 * <p>ScoreFunction可以设置weight和filter，控制打分的权重（function打分结果将会增加weight倍）以及筛选打分对象（仅经过filter筛选过的doc才会被此function打分）</p>
 */
public class ScoreFunction {
    /**
     * 控制weight
     */
    private Float weight;
    /**
     * 设置filter，filter与其他query类似，是一个子query
     */
    private Query filter;
    private FieldValueFactorFunction fieldValueFactorFunction;
    private DecayFunction decayFunction;
    private RandomFunction randomFunction;

    public ScoreFunction(){}

    public ScoreFunction(Float weight, Query filter, FieldValueFactorFunction fieldValueFactorFunction, DecayFunction decayFunction, RandomFunction randomFunction) {
        this.weight = weight;
        this.filter = filter;
        this.fieldValueFactorFunction = fieldValueFactorFunction;
        this.decayFunction = decayFunction;
        this.randomFunction = randomFunction;
    }

    public Float getWeight() {
        return weight;
    }

    public void setWeight(Float weight) {
        this.weight = weight;
    }

    public Query getFilter() {
        return filter;
    }

    public void setFilter(Query filter) {
        this.filter = filter;
    }

    public FieldValueFactorFunction getFieldValueFactorFunction() {
        return fieldValueFactorFunction;
    }

    public void setFieldValueFactorFunction(FieldValueFactorFunction fieldValueFactorFunction) {
        this.fieldValueFactorFunction = fieldValueFactorFunction;
    }

    public DecayFunction getDecayFunction() {
        return decayFunction;
    }

    public void setDecayFunction(DecayFunction decayFunction) {
        this.decayFunction = decayFunction;
    }

    public RandomFunction getRandomFunction() {
        return randomFunction;
    }

    public void setRandomFunction(RandomFunction randomFunction) {
        this.randomFunction = randomFunction;
    }

    public static ScoreFunction.Builder newBuilder() {
        return new ScoreFunction.Builder();
    }

    public static final class Builder {
        private Float weight;
        private Query filter;
        private FieldValueFactorFunction fieldValueFactorFunction;
        private DecayFunction decayFunction;
        private RandomFunction randomFunction;

        private Builder() {}

        public FieldValueFactorFunction fieldValueFactorFunction() {
            return fieldValueFactorFunction;
        }

        public ScoreFunction.Builder fieldValueFactorFunction(FieldValueFactorFunction fieldValueFactorFunction) {
            this.fieldValueFactorFunction = fieldValueFactorFunction;
            return this;
        }

        public DecayFunction decayFunction() {
            return decayFunction;
        }

        public ScoreFunction.Builder decayFunction(DecayFunction decayFunction) {
            this.decayFunction = decayFunction;
            return this;
        }

        public RandomFunction randomFunction() {
            return randomFunction;
        }

        public ScoreFunction.Builder randomFunction(RandomFunction randomFunction) {
            this.randomFunction = randomFunction;
            return this;
        }

        public ScoreFunction.Builder weight(float weight) {
            this.weight = weight;
            return this;
        }

        public Float weight() {
            return weight;
        }

        public Query filter() {
            return filter;
        }

        public ScoreFunction.Builder filter(Query filter) {
            this.filter = filter;
            return this;
        }

        public ScoreFunction build() {
            ScoreFunction scoreFunction = new ScoreFunction();
            scoreFunction.setDecayFunction(decayFunction);
            scoreFunction.setFilter(filter);
            scoreFunction.setFieldValueFactorFunction(fieldValueFactorFunction);
            scoreFunction.setRandomFunction(randomFunction);
            scoreFunction.setWeight(weight);
            return scoreFunction;
        }
    }
}
