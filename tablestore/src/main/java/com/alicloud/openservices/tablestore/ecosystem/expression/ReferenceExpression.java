package com.alicloud.openservices.tablestore.ecosystem.expression;

import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;

import java.util.List;

public class ReferenceExpression implements Expression {
    public ReferenceExpression(String refName, String sqlValueType) {
        this.refName = refName;
        this.sqlValueType = sqlValueType;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public List<Expression> getChildren() {
        return null;
    }

    private String refName;

    private PrimaryKeyValue pkv;

    private String sqlValueType;

    public String getRefName() {
        return refName;
    }

    public PrimaryKeyValue getPkv() {
        return pkv;
    }
}