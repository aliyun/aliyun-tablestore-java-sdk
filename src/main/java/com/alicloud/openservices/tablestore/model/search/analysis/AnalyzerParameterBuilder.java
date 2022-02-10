package com.alicloud.openservices.tablestore.model.search.analysis;

import com.alicloud.openservices.tablestore.core.protocol.Search;

public class AnalyzerParameterBuilder {
    public static Search.SingleWordAnalyzerParameter buildSingleWordAnalyzerParameter(SingleWordAnalyzerParameter param) {
        Search.SingleWordAnalyzerParameter.Builder builder = Search.SingleWordAnalyzerParameter.newBuilder();
        if (param.isCaseSensitive() != null) {
            builder.setCaseSensitive(param.isCaseSensitive());
        }
        if (param.isDelimitWord() != null) {
            builder.setDelimitWord(param.isDelimitWord());
        }
        return builder.build();
    }

    public static Search.SplitAnalyzerParameter buildSplitAnalyzerParameter(SplitAnalyzerParameter param) {
        Search.SplitAnalyzerParameter.Builder builder = Search.SplitAnalyzerParameter.newBuilder();
        if (param.getDelimiter() != null) {
            builder.setDelimiter(param.getDelimiter());
        }
        return builder.build();
    }

    public static Search.FuzzyAnalyzerParameter buildFuzzyAnalyzerParameter(FuzzyAnalyzerParameter param) {
        Search.FuzzyAnalyzerParameter.Builder builder = Search.FuzzyAnalyzerParameter.newBuilder();
        if (param.getMinChars() != null) {
            builder.setMinChars(param.getMinChars());
        }
        if (param.getMaxChars() != null) {
            builder.setMaxChars(param.getMaxChars());
        }
        return builder.build();
    }
}
