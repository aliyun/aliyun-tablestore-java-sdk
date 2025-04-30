package com.alicloud.openservices.tablestore.ecosystem.expression;

import java.util.List;

public class UnknownExpression implements Expression {
    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public List<Expression> getChildren() {
        return null;
    }

    private static UnknownExpression defaultUnknownExprInstance = new UnknownExpression();
    public static UnknownExpression defaultUnknownExpr() {
        return defaultUnknownExprInstance;
    }
}
