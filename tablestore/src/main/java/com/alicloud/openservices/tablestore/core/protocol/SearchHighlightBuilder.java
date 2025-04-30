package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.search.highlight.Highlight;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightEncoder;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightFragmentOrder;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightParameter;

import java.util.Map;

public class SearchHighlightBuilder {
    private static Search.HighlightEncoder buildHighlightEncoder(HighlightEncoder highlightEncoder) {
        switch (highlightEncoder) {
            case PLAIN:
                return Search.HighlightEncoder.PLAIN_MODE;
            case HTML:
                return Search.HighlightEncoder.HTML_MODE;
            default:
                throw new IllegalArgumentException("unknown highlight encoder type: " + highlightEncoder.name());
        }
    }

    private static Search.HighlightFragmentOrder buildHighlightFragmentOrder(HighlightFragmentOrder highlightFragmentOrder) {
        switch (highlightFragmentOrder) {
            case TEXT_SEQUENCE:
                return Search.HighlightFragmentOrder.TEXT_SEQUENCE;
            case SCORE:
                return Search.HighlightFragmentOrder.SCORE;
            default:
                throw new IllegalArgumentException("unknown highlight fragment order type: " + highlightFragmentOrder.name());
        }
    }

    public static Search.Highlight buildHighlight(Highlight highlight) {
        Search.Highlight.Builder pbHighlightBuilder = Search.Highlight.newBuilder();
        if (highlight.getHighlightEncoder() != null) {
            pbHighlightBuilder.setHighlightEncoder(buildHighlightEncoder(highlight.getHighlightEncoder()));
        }

        for (Map.Entry<String, HighlightParameter> entry : highlight.getFieldHighlightParams().entrySet()) {
            Search.HighlightParameter.Builder pbHighlightParamBuilder = Search.HighlightParameter.newBuilder();

            if (entry.getKey() != null) {
                pbHighlightParamBuilder.setFieldName(entry.getKey());
            }

            HighlightParameter highlightParameter = entry.getValue();
            if (highlightParameter == null) {
                pbHighlightBuilder.addHighlightParameters(pbHighlightParamBuilder);
                continue;
            }

            if (highlightParameter.getHighlightFragmentOrder() != null) {
                pbHighlightParamBuilder.setFragmentsOrder(buildHighlightFragmentOrder(highlightParameter.getHighlightFragmentOrder()));
            }
            if (highlightParameter.getFragmentSize() != null) {
                pbHighlightParamBuilder.setFragmentSize(highlightParameter.getFragmentSize());
            }
            if (highlightParameter.getNumberOfFragments() != null) {
                pbHighlightParamBuilder.setNumberOfFragments(highlightParameter.getNumberOfFragments());
            }
            if (highlightParameter.getPreTag() != null) {
                pbHighlightParamBuilder.setPreTag(highlightParameter.getPreTag());
            }
            if (highlightParameter.getPostTag() != null) {
                pbHighlightParamBuilder.setPostTag(highlightParameter.getPostTag());
            }

            pbHighlightBuilder.addHighlightParameters(pbHighlightParamBuilder);
        }
        return pbHighlightBuilder.build();
    }

    public static byte[] buildHighlightToBytes(Highlight highlight) {
        return buildHighlight(highlight).toByteArray();
    }
}
