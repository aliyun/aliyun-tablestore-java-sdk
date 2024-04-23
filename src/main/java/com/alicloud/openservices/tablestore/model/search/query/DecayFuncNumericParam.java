package com.alicloud.openservices.tablestore.model.search.query;

/**
 * 用于{@link DecayFunction}中设置参数，适用于Long和Double类型field。origin，scale和offset是Double类型值，其中scale应大于0，offset应大于等于0。
 */
public class DecayFuncNumericParam extends DecayParam {
    private final ParamType type = ParamType.NUMERIC;

    private Double origin;
    private Double scale;
    private Double offset;

    public DecayFuncNumericParam(){}

    public DecayFuncNumericParam(Double origin, Double scale, Double offset) {
        this.origin = origin;
        this.scale = scale;
        this.offset = offset;
    }

    public Double getOrigin() {
        return origin;
    }

    public void setOrigin(Double origin) {
        this.origin = origin;
    }

    public Double getScale() {
        return scale;
    }

    public void setScale(Double scale) {
        this.scale = scale;
    }

    public Double getOffset() {
        return offset;
    }

    public void setOffset(Double offset) {
        this.offset = offset;
    }

    @Override
    public ParamType getType() {
        return type;
    }

    public static DecayFuncNumericParam.Builder newBuilder() {
        return new DecayFuncNumericParam.Builder();
    }

    public static final class Builder {
        private final ParamType type = ParamType.NUMERIC;

        private Double origin;
        private Double scale;
        private Double offset;

        public Double origin() {
            return origin;
        }

        public DecayFuncNumericParam.Builder origin(double origin) {
            this.origin = origin;
            return this;
        }

        public Double scale() {
            return scale;
        }

        public DecayFuncNumericParam.Builder scale(double scale) {
            this.scale = scale;
            return this;
        }

        public Double offset() {
            return offset;
        }

        public DecayFuncNumericParam.Builder offset(double offset) {
            this.offset = offset;
            return this;
        }

        public ParamType type() {
            return type;
        }

        public DecayFuncNumericParam build() {
            DecayFuncNumericParam decayFuncNumericParam = new DecayFuncNumericParam();
            decayFuncNumericParam.setOffset(offset);
            decayFuncNumericParam.setScale(scale);
            decayFuncNumericParam.setOrigin(origin);
            return decayFuncNumericParam;
        }
    }
}
