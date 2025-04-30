package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.model.search.highlight.Highlight;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;

public class InnerHits {
    /**
     * Sorting rule for the return of nested sub-columns
     */
    private Sort sort;
    /**
     * The starting position for paginated returns of sub-columns when the nested sub-column is in array form.
     */
    private Integer offset;
    /**
     * The number of rows returned by sub-column pagination when the nested sub-column is in array form.
     */
    private Integer limit;

    /**
     * Configuration for nested sub-column highlighting parameters
     */
    private Highlight highlight;

    public void setSort(Sort sort) {
        this.sort = sort;
    }

    public Sort getSort() {
        return this.sort;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Integer getOffset() {
        return this.offset;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Integer getLimit() {
        return this.limit;
    }

    public void setHighlight(Highlight highlight) {
        this.highlight = highlight;
    }

    public Highlight getHighlight() {
        return this.highlight;
    }

    public static InnerHits.Builder newBuilder() {
        return new InnerHits.Builder();
    }

    public static final class Builder {
        private Sort sort;
        private Integer offset;
        private Integer limit;
        private Highlight highlight;

        public Builder sort(Sort sort) {
            this.sort = sort;
            return this;
        }

        public Builder offset(int offset) {
            this.offset = offset;
            return this;
        }

        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public Builder highlight(Highlight highlight) {
            this.highlight = highlight;
            return this;
        }

        public InnerHits build() {
            InnerHits innerHits = new InnerHits();
            innerHits.setSort(this.sort);
            innerHits.setOffset(this.offset);
            innerHits.setLimit(this.limit);
            innerHits.setHighlight(this.highlight);
            return innerHits;
        }
    }
}
