package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.search.query.InnerHits;

import static com.alicloud.openservices.tablestore.core.protocol.SearchHighlightBuilder.buildHighlight;
import static com.alicloud.openservices.tablestore.core.protocol.SearchSortBuilder.buildSort;

public class SearchInnerHitsBuilder {
    public static Search.InnerHits buildInnerHits(InnerHits innerHits) {
        Search.InnerHits.Builder pbInnerHits = Search.InnerHits.newBuilder();
        if (innerHits.getOffset() != null) {
            pbInnerHits.setOffset(innerHits.getOffset());
        }

        if (innerHits.getLimit() != null) {
            pbInnerHits.setLimit(innerHits.getLimit());
        }

        if (innerHits.getHighlight() != null) {
            pbInnerHits.setHighlight(buildHighlight(innerHits.getHighlight()));
        }

        if (innerHits.getSort() != null) {
            pbInnerHits.setSort(buildSort(innerHits.getSort()));
        }

        return pbInnerHits.build();
    }

    public static byte[] buildInnerHitsToBytes(InnerHits innerHits) {
        return buildInnerHits(innerHits).toByteArray();
    }
}
