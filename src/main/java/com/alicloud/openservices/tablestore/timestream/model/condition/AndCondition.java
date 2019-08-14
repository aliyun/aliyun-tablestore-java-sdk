package com.alicloud.openservices.tablestore.timestream.model.condition;

import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * and组合过滤器
 */
public class AndCondition implements Condition {
    private List<Condition> conditionList;

    public AndCondition(Condition... conditions) {
        this.conditionList = Arrays.asList(conditions);
    }

    public Query getQuery() {
        List<Query> queryList = new ArrayList<Query>();
        for (Condition c : conditionList) {
            queryList.add(c.getQuery());
        }
        BoolQuery boolQuery = new BoolQuery();
        boolQuery.setMustQueries(queryList);
        return boolQuery;
    }
}
