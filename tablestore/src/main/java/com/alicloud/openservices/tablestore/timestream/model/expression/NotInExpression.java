package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;

import java.util.ArrayList;
import java.util.List;

import static com.alicloud.openservices.tablestore.timestream.internal.Utils.buildTagValue;

public class NotInExpression implements Expression {
    private ColumnValue[] valueList;

    public NotInExpression(ColumnValue[] valueList){
        this.valueList = valueList;
    }

    public ColumnValue[] getValueList() {
        return valueList;
    }

    @Override
    public Query getQuery(String columnName) {
        List<Query> queryList = new ArrayList<Query>();
        for (ColumnValue value : valueList) {
            TermQuery termQuery = new TermQuery();
            termQuery.setFieldName(columnName);
            termQuery.setTerm(value);
            queryList.add(termQuery);
        }
        BoolQuery boolQuery = new BoolQuery();
        boolQuery.setMustNotQueries(queryList);
        return boolQuery;
    }
}
