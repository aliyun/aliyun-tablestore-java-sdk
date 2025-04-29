package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.search.SearchHit;
import com.alicloud.openservices.tablestore.model.search.SearchInnerHit;
import com.alicloud.openservices.tablestore.model.search.query.InnerHits;

import java.io.IOException;

import static com.alicloud.openservices.tablestore.core.protocol.SearchHighlightParser.buildHighlightResultItem;
import static com.alicloud.openservices.tablestore.core.protocol.SearchHighlightParser.toHighlight;
import static com.alicloud.openservices.tablestore.core.protocol.SearchSortParser.toSort;

public class SearchInnerHitsParser {
    static InnerHits toInnerHits(Search.InnerHits pbInnerhits) throws IOException {
        InnerHits.Builder innerHitsBuilder = InnerHits.newBuilder();
        if (pbInnerhits.hasLimit()) {
            innerHitsBuilder.limit(pbInnerhits.getLimit());
        }
        if (pbInnerhits.hasOffset()) {
            innerHitsBuilder.offset(pbInnerhits.getOffset());
        }
        if (pbInnerhits.hasSort()) {
            innerHitsBuilder.sort(toSort(pbInnerhits.getSort()));
        }
        if (pbInnerhits.hasHighlight()) {
            innerHitsBuilder.highlight(toHighlight(pbInnerhits.getHighlight()));
        }

        return innerHitsBuilder.build();
    }

    static InnerHits toInnerHits(byte[] pbInnerHitsBytes) throws IOException {
        Search.InnerHits pbInnerHits = Search.InnerHits.parseFrom(pbInnerHitsBytes);
        return toInnerHits(pbInnerHits);
    }

    static SearchHit toSearchHit(Search.SearchHit pbSearchHits, Row row) {
        SearchHit searchHit = new SearchHit();
        searchHit.setRow(row);
        searchHit.setHighlightResultItem(buildHighlightResultItem(pbSearchHits));

        if (pbSearchHits.hasNestedDocOffset()) {
            searchHit.setOffset(pbSearchHits.getNestedDocOffset());
        }

        if (pbSearchHits.hasScore()) {
            searchHit.setScore(pbSearchHits.getScore());
        }

        for (Search.SearchInnerHit searchInnerHit : pbSearchHits.getSearchInnerHitsList()) {
            searchHit.addSearchInnerHit(searchInnerHit.getPath(), toSearchInnerHit(searchInnerHit));
        }
        return searchHit;
    }

    static SearchInnerHit toSearchInnerHit(Search.SearchInnerHit pbSearchInnerHit) {
        SearchInnerHit searchInnerHit = new SearchInnerHit();
        if (pbSearchInnerHit.hasPath()) {
            searchInnerHit.setPath(pbSearchInnerHit.getPath());
        }

        for (Search.SearchHit innerSearchHit : pbSearchInnerHit.getSearchHitsList()) {
            searchInnerHit.addSubSearchHit(toSearchHit(innerSearchHit, null));
        }

        return searchInnerHit;
    }
}
