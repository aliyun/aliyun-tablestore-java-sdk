package com.alicloud.openservices.tablestore.ecosystem.expression;

import java.util.List;

public class BinaryExpression implements Expression {
    public BinaryExpression(String opName, List<Expression> subExprs) {
        this.opName = opName;

        if (subExprs.size() != 2) {
            throw new IllegalArgumentException("size not math:" + subExprs.size());
        }

        left = subExprs.get(0);
        right = subExprs.get(1);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public List<Expression> getChildren() {
        return null;
    }

    private String opName;

    private Expression left;

    private Expression right;

    public String getOpName() {
        return opName;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }
}
