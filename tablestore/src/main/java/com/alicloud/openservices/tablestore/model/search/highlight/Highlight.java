package com.alicloud.openservices.tablestore.model.search.highlight;

import com.alicloud.openservices.tablestore.model.search.query.MatchAllQuery;

import java.util.HashMap;
import java.util.Map;

public class Highlight {
    /**
     * The encoding method for highlighted shards, applied to all fields that need highlighting.
     */
    private HighlightEncoder highlightEncoder = HighlightEncoder.PLAIN;
    /**
     * Field highlighting parameter settings, only supports fields with keyword queries in the Query.
     * For example: {@link MatchAllQuery} does not support highlighting.
     */
    private Map<String, HighlightParameter> fieldHighlightParams = new HashMap<String, HighlightParameter>();

    public Highlight() {
    }

    public Highlight(Map<String, HighlightParameter> fieldHighlightParams) {
        this.fieldHighlightParams = fieldHighlightParams;
    }

    public Highlight(HighlightEncoder highlightEncoder, Map<String, HighlightParameter> fieldHighlightParams) {
        this.highlightEncoder = highlightEncoder;
        this.fieldHighlightParams = fieldHighlightParams;
    }

    public Highlight(Highlight.Builder builder) {
        this(builder.highlightEncoder, builder.fieldHighlightParams);
    }

    public static Highlight.Builder newBuilder() {
        return new Highlight.Builder();
    }

    public HighlightEncoder getHighlightEncoder() {
        return highlightEncoder;
    }

    public Highlight setHighlightEncoder(HighlightEncoder highlightEncoder) {
        this.highlightEncoder = highlightEncoder;
        return this;
    }

    public Map<String, HighlightParameter> getFieldHighlightParams() {
        return fieldHighlightParams;
    }

    public Highlight setFieldHighlightParams(Map<String, HighlightParameter> fieldHighlightParams) {
        this.fieldHighlightParams = fieldHighlightParams;
        return this;
    }

    public Highlight addFieldHighlightParam(String fieldName, HighlightParameter highlightParameter) {
        this.fieldHighlightParams.put(fieldName, highlightParameter);
        return this;
    }

    public static final class Builder {
        private HighlightEncoder highlightEncoder = HighlightEncoder.PLAIN;

        private Map<String, HighlightParameter> fieldHighlightParams = new HashMap<String, HighlightParameter>();

        public Builder() {
        }

        /**
         * Set the highlight snippet encoding mode
         *
         * @param highlightEncoder
         * @return {@link Highlight.Builder}
         */
        public Builder highlightEncoder(HighlightEncoder highlightEncoder) {
            this.highlightEncoder = highlightEncoder;
            return this;
        }

        /**
         * Set the highlight field and highlight parameters
         *
         * @param fieldHighlightParams
         * @return {@link Highlight.Builder}
         */
        public Builder fieldHighlightParams(Map<String, HighlightParameter> fieldHighlightParams) {
            this.fieldHighlightParams = fieldHighlightParams;
            return this;
        }

        /**
         * Add highlight parameters for the corresponding highlighted field
         * @param fieldName
         * @param highlightParameter
         * @return {@link Highlight.Builder}
         */
        public Builder addFieldHighlightParam(String fieldName, HighlightParameter highlightParameter) {
            this.fieldHighlightParams.put(fieldName, highlightParameter);
            return this;
        }

        public Highlight build() {
            return new Highlight(this);
        }
    }
}
