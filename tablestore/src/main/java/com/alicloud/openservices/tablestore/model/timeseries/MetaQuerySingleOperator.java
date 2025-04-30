package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;

public enum MetaQuerySingleOperator {

    OP_EQUAL,
    OP_NOT_EQUAL,
    OP_GREATER_THAN,
    OP_GREATER_EQUAL,
    OP_LESS_THAN,
    OP_LESS_EQUAL,
    OP_PREFIX;

    public Timeseries.MetaQuerySingleOperator toPB() {
        switch (this) {
            case OP_EQUAL: {
                return Timeseries.MetaQuerySingleOperator.OP_EQUAL;
            }
            case OP_GREATER_THAN: {
                return Timeseries.MetaQuerySingleOperator.OP_GREATER_THAN;
            }
            case OP_GREATER_EQUAL: {
                return Timeseries.MetaQuerySingleOperator.OP_GREATER_EQUAL;
            }
            case OP_LESS_THAN: {
                return Timeseries.MetaQuerySingleOperator.OP_LESS_THAN;
            }
            case OP_LESS_EQUAL: {
                return Timeseries.MetaQuerySingleOperator.OP_LESS_EQUAL;
            }
            case OP_PREFIX: {
                return Timeseries.MetaQuerySingleOperator.OP_PREFIX;
            }
            default: {
                throw new IllegalArgumentException();
            }
        }
    }
}
