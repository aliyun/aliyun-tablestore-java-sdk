package com.alicloud.openservices.tablestore.model.search.query;

/**
 * <p>在{@link FunctionsScoreQuery}中使用，该函数用于根据field与目标值的相对距离打分，可以对Geo-point、Date、Long和Double类型field打分。与之相对应的，decayParam分为
 * {@link DecayFuncGeoParam}、{@link DecayFuncDateParam}和{@link DecayFuncNumericParam}三种类型的参数设置，请根据field类型选择对应{@link DecayParam}。</p>
 * <p>param中的origin、scale和offset与DecayFunction中的decay共同用于计算分数，其中origin是打分的参照，scale和decay设置分数衰减标准
 * （与origin相对距离为scale的文档获得的分值为decay，与origin相距距离小于offset的文档同样会获得最高分1分）。</p>
 * <p>打分使用的函数包括EXP、GAUSS和LINEAR三种，由{@link MathFunction}控制，对于数组类型的field，使用{@link MultiValueMode}设置打分模式，MIN表示选取数组中最小值作为打分依据，以此类推……</p>
 * <p>tips:DecayFunction对某个field进行打分时，如果某个文档没有对应的field，则该文档会获得1分（最高分），为了避免受到干扰，
 * 建议在{@link FunctionsScoreQuery}的query中设置{@link ExistsQuery}类型Query，屏蔽影响。</p>
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
         * 高斯函数
         */
        GAUSS,
        /**
         * 指数函数
         */
        EXP,
        /**
         * 线性函数
         */
        LINEAR
    }

}
