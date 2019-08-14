package com.alicloud.openservices.tablestore.timestream.model.condition;

public class ConditionFactory {

    public static Condition and(Condition... filters) {
        return new AndCondition(filters);
    }

    public static Condition or(Condition... filters) {
        return new OrCondition(filters);
    }
}
