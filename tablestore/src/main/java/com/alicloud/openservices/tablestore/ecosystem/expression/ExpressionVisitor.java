package com.alicloud.openservices.tablestore.ecosystem.expression;

public interface ExpressionVisitor<T> {
    T visit (ReferenceExpression expr);
    T visit (LiteralExpression expr);
    T visit (BinaryExpression expr);
    T visit (LogicExpression expr);
    T visit(UnknownExpression expr);
}
