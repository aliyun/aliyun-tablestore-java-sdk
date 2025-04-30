package com.alicloud.openservices.tablestore.ecosystem.expression;

import java.util.List;

public interface Expression {
    <T>  T accept(ExpressionVisitor<T> visitor);

    List<Expression> getChildren();
}

