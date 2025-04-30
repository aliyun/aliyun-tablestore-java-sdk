package com.alicloud.openservices.tablestore.model.condition;

import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * TableStore supports conditional update functionality, allowing you to set a condition {@link com.alicloud.openservices.tablestore.model.Condition} when performing PutRow, UpdateRow, DeleteRow, or BatchWriteRow operations.
 * A Condition includes row existence expectations {@link com.alicloud.openservices.tablestore.model.RowExistenceExpectation} and column conditions {@link ColumnCondition}.
 *
 * {@link ColumnCondition} currently includes {@link SingleColumnValueCondition} and {@link CompositeColumnValueCondition}, 
 * the former is used to set a condition on a specific column value, such as "the value of column A is greater than 0", while the latter can set composite column value conditions, for example, "the value of column A is greater than 0 and the value of column B is less than 'abc'".
 */
public interface ColumnCondition {

    ColumnConditionType getConditionType();

    ByteString serialize();
}
