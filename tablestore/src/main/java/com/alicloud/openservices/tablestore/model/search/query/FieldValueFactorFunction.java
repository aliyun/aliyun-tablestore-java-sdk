package com.alicloud.openservices.tablestore.model.search.query;

/**
 * <p>This is an upgraded feature of {@link FieldValueFactor}.</p>
 * It is used in {@link FunctionsScoreQuery}, and the function's purpose is to score a doc by performing simple arithmetic operations on a specific field (which must be of type long or double).
 * <p>For example: In the query of a FunctionsScoreQuery, use a match query to find students whose names contain "Ming", but you want to sort the results by height. At this point, this function can be used. Set the fieldName to height, multiply the factor with the height field to control the weight, use modifier to control the scoring algorithm, including operations like squaring, square root, logarithm, etc. The missing parameter is used to set the default value for the field.</p>
 * <p>Operation example: fieldName: height, factor: 1.2f, modifier: LOG1P, then score = LOG1P(1.2f * height)</p>
 */
public class FieldValueFactorFunction {
    private String fieldName;
    private Float factor;
    private FunctionModifier modifier;
    private Double missing;

    public FieldValueFactorFunction(){}

    public FieldValueFactorFunction(String fieldName, Float factor, FunctionModifier modifier, Double missing) {
        this.fieldName = fieldName;
        this.factor = factor;
        this.modifier = modifier;
        this.missing = missing;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Float getFactor() {
        return factor;
    }

    public void setFactor(Float factor) {
        this.factor = factor;
    }

    public FunctionModifier getModifier() {
        return modifier;
    }

    public void setModifier(FunctionModifier modifier) {
        this.modifier = modifier;
    }

    public Double getMissing() {
        return missing;
    }

    public void setMissing(Double missing) {
        this.missing = missing;
    }

    public static FieldValueFactorFunction.Builder newBuilder() {
        return new FieldValueFactorFunction.Builder();
    }

    public static final class Builder {
        private String fieldName;
        private Float factor;
        private FunctionModifier modifier;
        private Double missing;

        private Builder() {}

        public String fieldName() {
            return fieldName;
        }

        public FieldValueFactorFunction.Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Float factor() {
            return factor;
        }

        public FieldValueFactorFunction.Builder factor(float factor) {
            this.factor = factor;
            return this;
        }

        public FunctionModifier modifier() {
            return modifier;
        }

        public FieldValueFactorFunction.Builder modifier(FunctionModifier modifier) {
            this.modifier = modifier;
            return this;
        }

        public Double missing() {
            return missing;
        }

        public FieldValueFactorFunction.Builder missing(double missing) {
            this.missing = missing;
            return this;
        }

        public FieldValueFactorFunction build() {
            FieldValueFactorFunction fieldValueFactorFunction = new FieldValueFactorFunction();
            fieldValueFactorFunction.setFieldName(fieldName);
            fieldValueFactorFunction.setFactor(factor);
            fieldValueFactorFunction.setModifier(modifier);
            fieldValueFactorFunction.setMissing(missing);
            return fieldValueFactorFunction;
        }
    }

    public enum FunctionModifier {
        UNKNOWN,
        /**
         * No additional operations
         */
        NONE,
        /**
         * Perform logarithmic calculation with base 10
         */
        LOG,
        /**
         * Take the base-10 logarithm after adding 1 to the actual value to prevent it from being 0.
         */
        LOG1P,
        /**
         * Take the logarithm with base 10 after adding 2 to the true number to prevent the true number from being 0.
         */
        LOG2P,
        /**
         * Perform natural logarithm operation
         */
        LN,
        /**
         * Take the natural logarithm after adding 1 to the argument, to prevent the argument from being 0
         */
        LN1P,
        /**
         * Add 2 to the logarithm and then take the natural logarithm with base e to prevent the logarithm from being 0
         */
        LN2P,
        /**
         * Square operation
         */
        SQUARE,
        /**
         * Square root operation
         */
        SQRT,
        /**
         * Reciprocal operation
         */
        RECIPROCAL
    }
}
