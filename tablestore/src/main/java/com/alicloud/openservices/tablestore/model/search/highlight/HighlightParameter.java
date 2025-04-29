package com.alicloud.openservices.tablestore.model.search.highlight;

public class HighlightParameter {
    /**
     * Returns the maximum number of highlighted shards
     */
    private Integer numberOfFragments;

    /**
     * The length of each shard. The actual length of the returned shards may not be strictly equal to this value.
     */
    private Integer fragmentSize;

    /**
     * The pre-tag for highlighting query keywords, such as <em>
     */
    private String preTag;

    /**
     * Query the post-tag for keyword highlighting, for example </em>
     */
    private String postTag;

    /**
     * The return order of multiple field shards
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
         * Set the maximum number of returned fragments for a single field
         *
         * @param numberOfFragments
         * @return  {@link Builder}
         */
        public Builder numberOfFragments(Integer numberOfFragments) {
            this.numberOfFragments = numberOfFragments;
            return this;
        }

        /**
         * Set the length of each shard. <b>Fuzzy value</b>
         *
         * @param fragmentSize
         * @return {@link Builder}
         */
        public Builder fragmentSize(Integer fragmentSize) {
            this.fragmentSize = fragmentSize;
            return this;
        }

        /**
         * Set the pre-tag for keyword highlighting, <b>must set postTag at the same time</b>
         *
         * @param preTag
         * @return {@link Builder}
         */
        public Builder preTag(String preTag) {
            this.preTag = preTag;
            return this;
        }

        /**
         * Set the post-tag for keyword highlighting, <b>must set preTag at the same time</b>
         *
         * @param postTag
         * @return {@link Builder}
         */
        public Builder postTag(String postTag) {
            this.postTag = postTag;
            return this;
        }

        /**
         * Set the sorting mode for multiple highlight fragments
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
