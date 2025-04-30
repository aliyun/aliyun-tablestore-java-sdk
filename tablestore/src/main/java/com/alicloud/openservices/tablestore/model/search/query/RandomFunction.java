package com.alicloud.openservices.tablestore.model.search.query;

/**
 * Used in {@link FunctionsScoreQuery}, this function can assign random scores to documents, returning a random sort order. The results returned each time will be different.
 */
public class RandomFunction {
    public RandomFunction() {}
    public static RandomFunction.Builder newBuilder() {
        return new RandomFunction.Builder();
    }

    public static final class Builder{
        private Builder() {}

        public RandomFunction build() {
            return new RandomFunction();
        }
    }

}
