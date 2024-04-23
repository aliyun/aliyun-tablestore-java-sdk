package com.alicloud.openservices.tablestore.model.search.query;

/**
 * 用于{@link DecayFunction}中设置参数，适用于Geo-point类型field。origin为经纬度坐标点，scale和offset是以米为单位的Double类型值，其中scale应大于0，offset应大于等于0。
 */
public class DecayFuncGeoParam extends DecayParam {
    private final ParamType type = ParamType.GEO;

    private String origin;
    private Double scale;
    private Double offset;

    public DecayFuncGeoParam(){}

    public DecayFuncGeoParam(String origin, Double scale, Double offset) {
        this.origin = origin;
        this.scale = scale;
        this.offset = offset;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
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

    public static DecayFuncGeoParam.Builder newBuilder() {
        return new DecayFuncGeoParam.Builder();
    }

    public static final class Builder {
        private final ParamType type = ParamType.GEO;

        private String origin;
        private Double scale;
        private Double offset;

        private Builder() {}

        public String origin() {
            return origin;
        }

        public DecayFuncGeoParam.Builder origin(String origin) {
            this.origin = origin;
            return this;
        }

        public DecayFuncGeoParam.Builder origin(double lon, double lat) {
            this.origin = "" + lon + "," + lat;
            return this;
        }

        public Double scale() {
            return scale;
        }

        public DecayFuncGeoParam.Builder scale(double scale) {
            this.scale = scale;
            return this;
        }

        public Double offset() {
            return offset;
        }

        public DecayFuncGeoParam.Builder offset(double offset) {
            this.offset = offset;
            return this;
        }

        public ParamType type() {
            return type;
        }

        public DecayFuncGeoParam build() {
            DecayFuncGeoParam decayFuncGeoParam = new DecayFuncGeoParam();
            decayFuncGeoParam.setOffset(offset);
            decayFuncGeoParam.setScale(scale);
            decayFuncGeoParam.setOrigin(origin);
            return decayFuncGeoParam;
        }
    }
}
