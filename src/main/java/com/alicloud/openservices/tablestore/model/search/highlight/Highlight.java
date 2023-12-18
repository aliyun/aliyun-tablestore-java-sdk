package com.alicloud.openservices.tablestore.model.search.highlight;

import com.alicloud.openservices.tablestore.model.search.query.MatchAllQuery;

import java.util.HashMap;
import java.util.Map;

public class Highlight {
    /**
     * 高亮分片的编码方式，作用于所有需要高亮的字段
     */
    private HighlightEncoder highlightEncoder = HighlightEncoder.PLAIN;
    /**
     * 字段高亮参数设置，仅支持设置Query中有关键字查询的字段。
     * 例如:{@link MatchAllQuery}查询不支持高亮
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
         * 设置高亮片段编码模式
         *
         * @param highlightEncoder
         * @return {@link Highlight.Builder}
         */
        public Builder highlightEncoder(HighlightEncoder highlightEncoder) {
            this.highlightEncoder = highlightEncoder;
            return this;
        }

        /**
         * 设置高亮字段及高亮参数
         *
         * @param fieldHighlightParams
         * @return {@link Highlight.Builder}
         */
        public Builder fieldHighlightParams(Map<String, HighlightParameter> fieldHighlightParams) {
            this.fieldHighlightParams = fieldHighlightParams;
            return this;
        }

        /**
         * 添加高亮字段对应的高亮参数
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
