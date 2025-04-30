package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.search.highlight.Highlight;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightEncoder;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightField;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightFragmentOrder;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightParameter;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightResultItem;
import com.aliyun.ots.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

public class SearchHighlightParser {
    public static Highlight toHighlight(byte[] pbHighlightBytes) throws InvalidProtocolBufferException {
        Search.Highlight pbHighlight = Search.Highlight.parseFrom(pbHighlightBytes);
        return toHighlight(pbHighlight);
    }

    public static Highlight toHighlight(Search.Highlight pbHighlight) {
        Highlight highlight = new Highlight();
        if (pbHighlight.hasHighlightEncoder()) {
            highlight.setHighlightEncoder(toHighlightEncoder(pbHighlight.getHighlightEncoder()));
        }

        for (Search.HighlightParameter pbHighlightParameter : pbHighlight.getHighlightParametersList()) {
            String fieldName = null;
            if (pbHighlightParameter.hasFieldName()) {
                fieldName = pbHighlightParameter.getFieldName();
            }

            HighlightParameter highlightParameter = new HighlightParameter();
            if (pbHighlightParameter.hasFragmentSize()) {
                highlightParameter.setFragmentSize(pbHighlightParameter.getFragmentSize());
            }
            if (pbHighlightParameter.hasFragmentsOrder()) {
                highlightParameter.setHighlightFragmentOrder(toHighlightFragmentOrder(pbHighlightParameter.getFragmentsOrder()));
            }
            if (pbHighlightParameter.hasNumberOfFragments()) {
                highlightParameter.setNumberOfFragments(pbHighlightParameter.getNumberOfFragments());
            }
            if (pbHighlightParameter.hasPreTag()) {
                highlightParameter.setPreTag(pbHighlightParameter.getPreTag());
            }
            if (pbHighlightParameter.hasPostTag()) {
                highlightParameter.setPostTag(pbHighlightParameter.getPostTag());
            }
            highlight.addFieldHighlightParam(fieldName, highlightParameter);
        }

        return highlight;
    }

    private static HighlightEncoder toHighlightEncoder(Search.HighlightEncoder encoder) {
        switch (encoder) {
            case PLAIN_MODE:
                return HighlightEncoder.PLAIN;
            case HTML_MODE:
                return HighlightEncoder.HTML;
            default:
                throw new IllegalArgumentException("unknown highlight encoder type: " + encoder.name());
        }
    }

    private static HighlightFragmentOrder toHighlightFragmentOrder(Search.HighlightFragmentOrder order) {
        switch (order) {
            case TEXT_SEQUENCE:
                return HighlightFragmentOrder.TEXT_SEQUENCE;
            case SCORE:
                return HighlightFragmentOrder.SCORE;
            default:
                throw new IllegalArgumentException("unknown highlight fragment order type: " + order.name());
        }
    }

    static HighlightResultItem buildHighlightResultItem(Search.SearchHit searchHit) {
        HighlightResultItem highlightResultItem = new HighlightResultItem();
        if (searchHit.hasHighlightResult()) {
            Search.HighlightResult pbHighlightResult = searchHit.getHighlightResult();
            for (Search.HighlightField pbHighlightField : pbHighlightResult.getHighlightFieldsList()) {
                HighlightField highlightField = new HighlightField();
                highlightField.setFragments(pbHighlightField.getFieldFragmentsList());
                highlightResultItem.addHighlightField(pbHighlightField.getFieldName(), highlightField);
            }
        }
        return highlightResultItem;
    }
}
