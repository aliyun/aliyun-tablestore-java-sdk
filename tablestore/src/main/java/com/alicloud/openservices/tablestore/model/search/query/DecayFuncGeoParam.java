package com.alicloud.openservices.tablestore.model.search.query;

/**
 * Used in {@link DecayFunction} to set parameters, applicable to Geo-point type fields. 
 * The origin is a longitude and latitude coordinate point, and both scale and offset are Double type values in meters. 
 * The scale should be greater than 0, and the offset should be greater than or equal to 0.
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
