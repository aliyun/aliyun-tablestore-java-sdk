package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermsQuery;

import java.util.ArrayList;
import java.util.List;

import static com.alicloud.openservices.tablestore.timestream.internal.Utils.buildTagValue;

public class InExpression implements Expression  {
    private ColumnValue[] valueList;

    public InExpression(ColumnValue[] valueList){
        this.valueList = valueList;
    }

    public ColumnValue[] getValueList() {
        return valueList;
    }

    @Override
    public Query getQuery(String columnName) {
        TermsQuery termsQuery = new TermsQuery();
        termsQuery.setFieldName(columnName);
        for (ColumnValue value : valueList) {
            termsQuery.addTerm(value);
        }
        return termsQuery;
    }
}
