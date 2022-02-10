package com.alicloud.openservices.tablestore.model.search.analysis;

import com.google.protobuf.ByteString;

public class SplitAnalyzerParameter implements AnalyzerParameter {
    private String delimiter = null;

    public SplitAnalyzerParameter() {
    }

    public SplitAnalyzerParameter(final String delimiter) {
        this.delimiter = delimiter;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public SplitAnalyzerParameter setDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    @Override
    public ByteString serialize() {
        return AnalyzerParameterBuilder.buildSplitAnalyzerParameter(this).toByteString();
    }
}
