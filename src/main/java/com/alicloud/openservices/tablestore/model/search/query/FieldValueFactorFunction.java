package com.alicloud.openservices.tablestore.model.search.query;

/**
 * <p>是{@link FieldValueFactor}的升级功能。</p>
 * 在{@link FunctionsScoreQuery}中使用，该函数的功能是对doc中的某个field（必须为long或者double类型）简单运算打分。
 * <p>例如：在FunctionsScoreQuery的query中使用match query查询姓名中含有“明”的同学，但是想对返回结果按照身高进行排序，此时可以使用此函数，在fieldName字段设置身高，factor与身高field
 * 相乘，控制权重，modifier控制打分算法，包括平方、开方、取对数等简单运算，missing用于设置field缺省值。</p>
 * <p>运算举例：fieldName：height，factor：1.2f，modifier：LOG1P，则score = LOG1P(1.2f * height)</p>
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
         * 不做额外运算
         */
        NONE,
        /**
         * 取10为底对数运算
         */
        LOG,
        /**
         * 对真数加1后取10为底对数，防止真数为0
         */
        LOG1P,
        /**
         * 对真数加2后取10为底对数，防止真数为0
         */
        LOG2P,
        /**
         * 取e为底对数运算
         */
        LN,
        /**
         * 对真数加1后取e为底对数，防止真数为0
         */
        LN1P,
        /**
         * 对真数加2后取e为底对数，防止真数为0
         */
        LN2P,
        /**
         * 平方运算
         */
        SQUARE,
        /**
         * 开方运算
         */
        SQRT,
        /**
         * 倒数运算
         */
        RECIPROCAL
    }
}
