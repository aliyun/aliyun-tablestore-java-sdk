/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 *
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model.condition;

import com.google.protobuf.ByteString;

public interface ColumnCondition {

    public ColumnConditionType getType();

    public ByteString serialize();
}
