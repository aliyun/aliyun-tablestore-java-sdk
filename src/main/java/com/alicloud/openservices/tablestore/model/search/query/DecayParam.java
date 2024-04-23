package com.alicloud.openservices.tablestore.model.search.query;

/**
 * 用于{@link DecayFunction}中设置参数，可选的参数类型包括{@link DecayFuncGeoParam}、{@link DecayFuncDateParam}和{@link DecayFuncNumericParam}三种。
 */
public abstract class DecayParam {

    /**
     * 获取param类型
     * @return param类型
     */
    public abstract ParamType getType();

    public enum ParamType {
        UNKNOWN,
        /**
         * 适用于Date类型field
         */
        DATE,
        /**
         * 适用于Geo-point类型field
         */
        GEO,
        /**
         * 适用于Long和Double类型field
         */
        NUMERIC
    }

    public static DecayParam unknownTypeParam() {
        return new DecayFuncUnknownParam();
    }

    public static class DecayFuncUnknownParam extends DecayParam{
        private final ParamType type = ParamType.UNKNOWN;

        private DecayFuncUnknownParam() {}
        @Override
        public ParamType getType() {
            return type;
        }
    }
}
