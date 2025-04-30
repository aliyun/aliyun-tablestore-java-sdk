package com.alicloud.openservices.tablestore.model.search.query;

/**
 * Used in {@link DecayFunction} to set parameters. Optional parameter types include 
 * {@link DecayFuncGeoParam}, {@link DecayFuncDateParam}, and {@link DecayFuncNumericParam}.
 */
public abstract class DecayParam {

    /**
     * Get the type of param
     * @return the type of param
     */
    public abstract ParamType getType();

    public enum ParamType {
        UNKNOWN,
        /**
         * Applicable to Date type field
         */
        DATE,
        /**
         * Applicable to Geo-point type field
         */
        GEO,
        /**
         * Applicable to Long and Double type fields
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
