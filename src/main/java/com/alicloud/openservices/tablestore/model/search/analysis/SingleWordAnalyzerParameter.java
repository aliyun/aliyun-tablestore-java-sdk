package com.alicloud.openservices.tablestore.model.search.analysis;

import com.google.protobuf.ByteString;

public class SingleWordAnalyzerParameter implements AnalyzerParameter {
    private Boolean caseSensitive = null;
    private Boolean delimitWord = null;

    public SingleWordAnalyzerParameter() {
    }

    public SingleWordAnalyzerParameter(final Boolean caseSensitive, final Boolean delimitWord) {
        this.caseSensitive = caseSensitive;
        this.delimitWord = delimitWord;
    }

    public Boolean isCaseSensitive() {
        return caseSensitive;
    }

    public SingleWordAnalyzerParameter setCaseSensitive(Boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
        return this;
    }

    public Boolean isDelimitWord() {
        return delimitWord;
    }

    public SingleWordAnalyzerParameter setDelimitWord(Boolean delimitWord) {
        this.delimitWord = delimitWord;
        return this;
    }

    @Override
    public ByteString serialize() {
        return AnalyzerParameterBuilder.buildSingleWordAnalyzerParameter(this).toByteString();
    }
}
