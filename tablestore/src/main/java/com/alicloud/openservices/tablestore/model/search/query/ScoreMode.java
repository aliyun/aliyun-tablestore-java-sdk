package com.alicloud.openservices.tablestore.model.search.query;

/**
 * Specifies which value to use for sorting when a field contains multiple values.
 * <p>Example: Consider an elementary school student status monitoring system that records the height of students. Since the students' heights keep increasing, the "height" field is stored as an array. When querying, if we want to sort by height, we can set the ScoreMode to MAX, which will then return the most recent height.</p>
 */
public enum ScoreMode {
    None,
    Avg,
    Max,
    Min,
    Total,
}
