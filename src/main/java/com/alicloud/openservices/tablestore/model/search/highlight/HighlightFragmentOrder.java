package com.alicloud.openservices.tablestore.model.search.highlight;

/**
 * 高亮字段返回多个分片时，分片的排序规则。默认为片段在文本中出现的顺序。
 * SCORE: 根据命中关键词评分排序多个分片
 */
public enum HighlightFragmentOrder {
    TEXT_SEQUENCE,
    SCORE
}
