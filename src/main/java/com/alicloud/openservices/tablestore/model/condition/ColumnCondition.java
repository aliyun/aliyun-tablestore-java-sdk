package com.alicloud.openservices.tablestore.model.condition;

import com.google.protobuf.ByteString;

/**
 * TableStore支持条件更新功能, 在进行PutRow, UpdateRow, DeleteRow或BatchWriteRow操作时, 可以设置条件{@link com.alicloud.openservices.tablestore.model.Condition}.
 * Condition包括行存在性条件{@link com.alicloud.openservices.tablestore.model.RowExistenceExpectation}和列条件{@link ColumnCondition}.
 *
 * {@link ColumnCondition}目前包括{@link SingleColumnValueCondition} 和 {@link CompositeColumnValueCondition},
 * 前者用于设置某一列值的条件, 比如条件为"列A的值大于0", 后者可以设置组合列值条件, 比如" 列A的值大于0 且 列B的值小于'abc' ".
 */
public interface ColumnCondition {

    ColumnConditionType getConditionType();

    ByteString serialize();
}
