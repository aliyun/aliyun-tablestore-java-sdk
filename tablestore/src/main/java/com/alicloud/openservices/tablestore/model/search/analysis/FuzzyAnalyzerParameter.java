package com.alicloud.openservices.tablestore.model.search.analysis;

import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

public class FuzzyAnalyzerParameter implements AnalyzerParameter {
    private Integer minChars = null;
    private Integer maxChars = null;
    private Boolean caseSensitive = null;

    public FuzzyAnalyzerParameter() {
    }

    public FuzzyAnalyzerParameter(int minChars, int maxChars) {
        this.minChars = minChars;
        this.maxChars = maxChars;
    }

    public FuzzyAnalyzerParameter(Integer minChars, Integer maxChars, Boolean caseSensitive) {
        this.minChars = minChars;
        this.maxChars = maxChars;
        this.caseSensitive = caseSensitive;
    }

    public Boolean isCaseSensitive() {
        return caseSensitive;
    }

    public FuzzyAnalyzerParameter setCaseSensitive(Boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
        return this;
    }

    public Integer getMinChars() {
        return minChars;
    }

    public FuzzyAnalyzerParameter setMinChars(int minChars) {
        this.minChars = minChars;
        return this;
    }

    public Integer getMaxChars() {
        return maxChars;
    }

    public FuzzyAnalyzerParameter setMaxChars(int maxChars) {
        this.maxChars = maxChars;
        return this;
    }

    @Override
    public ByteString serialize() {
        return AnalyzerParameterBuilder.buildFuzzyAnalyzerParameter(this).toByteString();
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
