package com.alicloud.openservices.tablestore.model.search.highlight;

/**
 * The sorting rule for shards when the highlighted field returns multiple shards. By default, it is the order in which the fragments appear in the text.
 * SCORE: Sort multiple shards based on the score of matching keywords.
 */
public enum HighlightFragmentOrder {
    TEXT_SEQUENCE,
    SCORE
}
