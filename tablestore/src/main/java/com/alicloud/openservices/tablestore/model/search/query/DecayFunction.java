package com.alicloud.openservices.tablestore.model.search.query;

/**
 * <p>Used in {@link FunctionsScoreQuery}, this function scores based on the relative distance between a field and a target value. It can be used to score fields of Geo-point, Date, Long, and Double types. Correspondingly, the decayParam is divided into three types of parameter settings: 
 * {@link DecayFuncGeoParam}, {@link DecayFuncDateParam}, and {@link DecayFuncNumericParam}. Please choose the corresponding {@link DecayParam} according to the field type.</p>
 * <p>In the param, origin, scale, and offset are used together with decay in the DecayFunction to calculate the score. The origin serves as the reference for scoring, while scale and decay set the standard for score decay 
 * (documents at a relative distance of scale from the origin will receive a score equal to decay, and documents within a distance less than offset from the origin will also receive the maximum score of 1).</p>
 * <p>The scoring functions include EXP, GAUSS, and LINEAR, controlled by {@link MathFunction}. For array-type fields, use {@link MultiValueMode} to set the scoring mode; MIN indicates selecting the smallest value in the array as the scoring basis, and so on...</p>
 * <p>Tips: When using DecayFunction to score a specific field, if a document does not have the corresponding field, it will receive the maximum score of 1. To avoid interference, 
 * it is recommended to set an {@link ExistsQuery} type Query in the query of {@link FunctionsScoreQuery} to filter out such impacts.</p>
 */
public class DecayFunction {
    private String fieldName;
    private DecayParam decayParam;
    private MathFunction mathFunction;
    private Double decay;
    private MultiValueMode multiValueMode;

    public DecayFunction(){}

    public DecayFunction(String fieldName, DecayParam decayParam, MathFunction mathFunction, Double decay, MultiValueMode multiValueMode) {
        this.fieldName = fieldName;
        this.decayParam = decayParam;
        this.mathFunction = mathFunction;
        this.decay = decay;
        this.multiValueMode = multiValueMode;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public DecayParam getDecayParam() {
        return decayParam;
    }

    public void setDecayParam(DecayParam decayParam) {
        this.decayParam = decayParam;
    }

    public MathFunction getMathFunction() {
        return mathFunction;
    }

    public void setMathFunction(MathFunction mathFunction) {
        this.mathFunction = mathFunction;
    }

    public Double getDecay() {
        return decay;
    }

    public void setDecay(Double decay) {
        this.decay = decay;
    }

    public MultiValueMode getMultiValueMode() {
        return multiValueMode;
    }

    public void setMultiValueMode(MultiValueMode multiValueMode) {
        this.multiValueMode = multiValueMode;
    }

    public static DecayFunction.Builder newBuilder() {
        return new DecayFunction.Builder();
    }

    public static final class Builder {
        private String fieldName;
        private DecayParam decayParam;
        private MathFunction mathFunction;
        private Double decay;
        private MultiValueMode multiValueMode;

        private Builder() {}

        public String fieldName() {
            return fieldName;
        }

        public DecayFunction.Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public DecayParam decayParam() {
            return decayParam;
        }

        public DecayFunction.Builder decayParam(DecayParam decayParam) {
            this.decayParam = decayParam;
            return this;
        }

        public MathFunction mathFunction() {
            return mathFunction;
        }

        public DecayFunction.Builder mathFunction(MathFunction mathFunction) {
            this.mathFunction = mathFunction;
            return this;
        }

        public Double decay() {
            return decay;
        }

        public DecayFunction.Builder decay(double decay) {
            this.decay = decay;
            return this;
        }

        public MultiValueMode multiValueMode() {
            return multiValueMode;
        }

        public DecayFunction.Builder multiValueMode(MultiValueMode multiValueMode) {
            this.multiValueMode = multiValueMode;
            return this;
        }

        public DecayFunction build() {
            DecayFunction decayFunction = new DecayFunction();
            decayFunction.setMathFunction(mathFunction);
            decayFunction.setDecay(decay);
            decayFunction.setDecayParam(decayParam);
            decayFunction.setMultiValueMode(multiValueMode);
            decayFunction.setFieldName(fieldName);
            return decayFunction;
        }
    }

    public enum MathFunction {
        UNKNOWN,
        /**
         * Gaussian function
         */
        GAUSS,
        /**
         * Exponential function
         */
        EXP,
        /**
         * Linear function
         */
        LINEAR
    }

}
