package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.model.search.DateTimeUnit;
import com.alicloud.openservices.tablestore.model.search.DateTimeValue;

/**
 * <p>用于{@link DecayFunction}中设置参数，适用于Date类型field。origin设置时可以选择Long类型的纳秒时间戳，或者String类型、符合时间format的字符串，请任选其一设置。scale和offset请使用{@link DateTimeValue}类型时间设置，最大支持的时间单位为DateTimeUnit.DAY，其中scale应大于0，offset应大于等于0。</p>
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
