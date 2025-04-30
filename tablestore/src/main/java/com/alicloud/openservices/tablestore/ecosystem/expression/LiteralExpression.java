package com.alicloud.openservices.tablestore.ecosystem.expression;

import java.util.List;

public class LiteralExpression implements Expression {
    public LiteralExpression(String type, String value) {
        this.type = type;
        this.value = value;;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public List<Expression> getChildren() {
        return null;
    }

    private String type;
    private String value;

    public String getValue() {
        return value;
    }

    public String getType() {
        return type;
    }
}
