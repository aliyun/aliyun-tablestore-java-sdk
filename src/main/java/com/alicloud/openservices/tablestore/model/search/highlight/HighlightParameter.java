package com.alicloud.openservices.tablestore.model.search.highlight;

public class HighlightParameter {
    /**
     * 返回高亮分片的最大数量
     */
    private Integer numberOfFragments;

    /**
     * 每个分片的长度。实际返回分片的长度不会与改值严格相等
     */
    private Integer fragmentSize;

    /**
     * 查询关键词高亮的前置Tag，例如<em>
     */
    private String preTag;

    /**
     * 查询关键词高亮的后置Tag，例如</em>
     */
    private String postTag;

    /**
     * 多个字段分片的返回顺序
     */
    private HighlightFragmentOrder highlightFragmentOrder;

    public HighlightParameter() {
    }

    public HighlightParameter(HighlightParameter.Builder builder) {
        setNumberOfFragments(builder.numberOfFragments);
        setFragmentSize(builder.fragmentSize);
        setPreTag(builder.preTag);
        setPostTag(builder.postTag);
        setHighlightFragmentOrder(builder.highlightFragmentOrder);
    }

    public static HighlightParameter.Builder newBuilder() {
        return new HighlightParameter.Builder();
    }

    public Integer getNumberOfFragments() {
        return numberOfFragments;
    }

    public HighlightParameter setNumberOfFragments(Integer numberOfFragments) {
        this.numberOfFragments = numberOfFragments;
        return this;
    }

    public Integer getFragmentSize() {
        return fragmentSize;
    }

    public HighlightParameter setFragmentSize(Integer fragmentSize) {
        this.fragmentSize = fragmentSize;
        return this;
    }

    public String getPreTag() {
        return preTag;
    }

    public HighlightParameter setPreTag(String preTag) {
        this.preTag = preTag;
        return this;
    }

    public String getPostTag() {
        return postTag;
    }

    public HighlightParameter setPostTag(String postTag) {
        this.postTag = postTag;
        return this;
    }

    public HighlightFragmentOrder getHighlightFragmentOrder() {
        return highlightFragmentOrder;
    }

    public HighlightParameter setHighlightFragmentOrder(HighlightFragmentOrder highlightFragmentOrder) {
        this.highlightFragmentOrder = highlightFragmentOrder;
        return this;
    }

    public static final class Builder {
        private Integer numberOfFragments;

        private Integer fragmentSize;

        private String preTag;

        private String postTag;

        private HighlightFragmentOrder highlightFragmentOrder;

        public Builder() {
        }

        /**
         * 设置单个字段最大返回分片数量
         *
         * @param numberOfFragments
         * @return  {@link Builder}
         */
        public Builder numberOfFragments(Integer numberOfFragments) {
            this.numberOfFragments = numberOfFragments;
            return this;
        }

        /**
         * 设置每个分片的长度。<b>模糊值</b>
         *
         * @param fragmentSize
         * @return {@link Builder}
         */
        public Builder fragmentSize(Integer fragmentSize) {
            this.fragmentSize = fragmentSize;
            return this;
        }

        /**
         * 设置关键词高亮前置tag，<b>需同时设置postTag</b>
         *
         * @param preTag
         * @return {@link Builder}
         */
        public Builder preTag(String preTag) {
            this.preTag = preTag;
            return this;
        }

        /**
         * 设置关键词高亮后置tag，<b>需同时设置preTag</b>
         *
         * @param postTag
         * @return {@link Builder}
         */
        public Builder postTag(String postTag) {
            this.postTag = postTag;
            return this;
        }

        /**
         * 设置多个高亮分片的排序模式
         *
         * @param highlightFragmentOrder
         * @return {@link Builder}
         */
        public Builder highlightFragmentOrder(HighlightFragmentOrder highlightFragmentOrder) {
            this.highlightFragmentOrder = highlightFragmentOrder;
            return this;
        }

        public HighlightParameter build() {
            return new HighlightParameter(this);
        }
    }
}
