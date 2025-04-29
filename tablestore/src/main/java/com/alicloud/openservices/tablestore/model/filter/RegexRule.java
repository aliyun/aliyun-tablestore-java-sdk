package com.alicloud.openservices.tablestore.model.filter;

import com.alicloud.openservices.tablestore.core.protocol.*;

public class RegexRule {
    public enum CastType {
        VT_INTEGER,
        VT_DOUBLE,
        VT_STRING,
    }

    private String regex;
    private CastType castType = CastType.VT_INTEGER;

    public RegexRule(String regex, CastType type) {
        this.regex = regex;
        this.castType = type;
    }

    public String getRegex() {
        return this.regex;
    }

    public CastType getCastType() {
        return this.castType;
    }

    // support serialize to pb
    public OtsFilter.ValueTransferRule serialize() {
        OtsFilter.ValueTransferRule.Builder builder = OtsFilter.ValueTransferRule.newBuilder();
        builder.setRegex(this.regex);

        switch (this.castType) {
        case VT_INTEGER:
            builder.setCastType(OtsFilter.VariantType.VT_INTEGER);
            break;
        case VT_DOUBLE:
            builder.setCastType(OtsFilter.VariantType.VT_DOUBLE);
            break;
        case VT_STRING:
            builder.setCastType(OtsFilter.VariantType.VT_STRING);
            break;
        default:
            throw new IllegalArgumentException("Unknown variant type: " + this.castType);
        }
        return builder.build();
    }
}
