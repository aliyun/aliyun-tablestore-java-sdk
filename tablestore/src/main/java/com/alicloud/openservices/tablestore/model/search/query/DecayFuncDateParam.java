package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.model.search.DateTimeUnit;
import com.alicloud.openservices.tablestore.model.search.DateTimeValue;

/**
 * <p>Used for setting parameters in {@link DecayFunction}, applicable to Date type fields. When setting origin, you can choose either a Long type nanosecond timestamp or a String type that conforms to the time format string; please set one of them. For scale and offset, please use the {@link DateTimeValue} type for time settings. The largest supported time unit is DateTimeUnit.DAY, where scale should be greater than 0 and offset should be greater than or equal to 0.</p>
 */
public class DecayFuncDateParam extends DecayParam {
    private final ParamType type = ParamType.DATE;

    private Long originLong;
    private String originString;
    private DateTimeValue scale;
    private DateTimeValue offset;

    public DecayFuncDateParam(){}

    public DecayFuncDateParam(Long originLong, String originString, DateTimeValue scale, DateTimeValue offset) {
        this.originLong = originLong;
        this.originString = originString;
        this.scale = scale;
        this.offset = offset;
    }

    public Long getOriginLong() {
        return originLong;
    }

    public void setOriginLong(Long originLong) {
        this.originLong = originLong;
    }

    public String getOriginString() {
        return originString;
    }

    public void setOriginString(String originString) {
        this.originString = originString;
    }

    public DateTimeValue getScale() {
        return scale;
    }

    public void setScale(DateTimeValue scale) {
        this.scale = scale;
    }

    public DateTimeValue getOffset() {
        return offset;
    }

    public void setOffset(DateTimeValue offset) {
        this.offset = offset;
    }

    @Override
    public ParamType getType() {
        return type;
    }

    public static DecayFuncDateParam.Builder newBuilder() {
        return new DecayFuncDateParam.Builder();
    }

    public static final class Builder {
        private final ParamType type = ParamType.DATE;

        private Long originLong;
        private String originString;
        private DateTimeValue scale;
        private DateTimeValue offset;

        private Builder() {}

        public Long originLong() {
            return originLong;
        }

        public DecayFuncDateParam.Builder originLong(long originLong) {
            this.originLong = originLong;
            return this;
        }

        public String originString() {
            return originString;
        }

        public DecayFuncDateParam.Builder originString(String originString) {
            this.originString = originString;
            return this;
        }

        public DateTimeValue scale() {
            return scale;
        }

        public DecayFuncDateParam.Builder scale(DateTimeValue scale) {
            this.scale = scale;
            return this;
        }

        public DecayFuncDateParam.Builder scale(Integer value, DateTimeUnit unit) {
            this.scale = new DateTimeValue(value, unit);
            return this;
        }

        public DateTimeValue offset() {
            return offset;
        }

        public DecayFuncDateParam.Builder offset(DateTimeValue offset) {
            this.offset = offset;
            return this;
        }

        public DecayFuncDateParam.Builder offset(Integer value, DateTimeUnit unit) {
            this.offset = new DateTimeValue(value, unit);
            return this;
        }

        public ParamType type() {
            return type;
        }

        public DecayFuncDateParam build() {
            DecayFuncDateParam decayFuncDateParam = new DecayFuncDateParam();
            decayFuncDateParam.setOffset(offset);
            decayFuncDateParam.setScale(scale);
            decayFuncDateParam.setOriginString(originString);
            decayFuncDateParam.setOriginLong(originLong);
            return decayFuncDateParam;
        }
    }
}
