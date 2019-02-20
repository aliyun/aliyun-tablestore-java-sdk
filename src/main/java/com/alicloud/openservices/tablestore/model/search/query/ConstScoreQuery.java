package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 当我们不关心检索词频率TF（Term Frequency）对搜索结果排序的影响时，可以使用constant_score将查询语句query或者过滤语句filter包装起来，达到提高搜索速度。
 * <p>举例：我们班有100个人，有一个字段叫“name”，我们想要获得名字中包含“王”的人，我们并不关心排序结果，使用ConstScoreQuery（将原来的Query放在" private Query filter;"中）将会大大提高搜索速度。</p>
 */
public class ConstScoreQuery implements Query {

    private Query filter;

    public Query getFilter() {
        return filter;
    }

    public void setFilter(Query filter) {
        this.filter = filter;
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.QueryType_ConstScoreQuery;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildConstScoreQuery(this).toByteString();
    }
}
