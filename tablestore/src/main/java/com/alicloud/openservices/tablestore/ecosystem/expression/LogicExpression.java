package com.alicloud.openservices.tablestore.ecosystem.expression;

import java.util.List;

public class LogicExpression implements Expression {
    private String name;
    private List<Expression> subExprs;

    public LogicExpression(String name, List<Expression> subExprs) {
        this.name = name;
        this.subExprs = subExprs;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public List<Expression> getChildren() {
        return this.subExprs;
    }

    public void addSubExpression(Expression expr) {
        subExprs.add(expr);
    }

    public String getName() {
        return this.name;
    }

    public static final String AND_OP_NAME = "AND";
    public static final String OR_OP_NAME = "OR";
}
