package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 获取所有的文档，所有文档分数为1。返回的结果中：命中数永远都是正确的。加入返回的结果过多，SearchIndex会只返回部分数据。
 */
public class MatchAllQuery implements Query {

    @Override
    public QueryType getQueryType() {
        return QueryType.QueryType_MatchAllQuery;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildMatchAllQuery().toByteString();
    }
}
