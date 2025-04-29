package com.alicloud.openservices.tablestore.model.search.query;

/**
 * <p>Each ScoreFunction is a scoring function, and currently, up to three ScoreFunctions can be used simultaneously for scoring.</p>
 * <p>Each ScoreFunction includes three types of functions: {@link FieldValueFactorFunction}, {@link DecayFunction}, and {@link RandomFunction}. Please select one of these to configure or leave all unset (use only filter and weight).</p>
 * <p>The ScoreFunction can set weight and filter, controlling the weight of the scoring (the result of the function scoring will be multiplied by weight) and filtering the scoring objects (only docs that pass the filter will be scored by this function).</p>
 */
public class ScoreFunction {
    /**
     * Control weight
     */
    private Float weight;
    /**
     * Set the filter, which is similar to other queries and is a sub-query.
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
