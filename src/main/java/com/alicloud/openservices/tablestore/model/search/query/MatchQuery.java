package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 包括模糊匹配和短语或邻近查询
 */
public class MatchQuery implements Query {
    /**
     * 字段
     */
    private String fieldName;
    /**
     * 模糊匹配的值
     */
    private String text;

    /**
     * 最小匹配个数
     * @return
     */
    private Integer minimumShouldMatch;

    /**
     * 操作符
     * @return
     */
    private QueryOperator operator;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public QueryOperator getOperator() {
        return operator;
    }

    public void setOperator(QueryOperator operator) {
        this.operator = operator;
    }

    public Integer getMinimumShouldMatch() {
        return minimumShouldMatch;
    }

    public void setMinimumShouldMatch(Integer minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.QueryType_MatchQuery;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildMatchQuery(this).toByteString();
    }

}
