package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;

public enum MetaQueryCompositeOperator {
    OP_AND,
    OP_OR,
    OP_NOT;

    public Timeseries.MetaQueryCompositeOperator toPB() {
        switch (this) {
            case OP_AND: {
                return Timeseries.MetaQueryCompositeOperator.OP_AND;
            }
            case OP_OR: {
                return Timeseries.MetaQueryCompositeOperator.OP_OR;
            }
            case OP_NOT: {
                return Timeseries.MetaQueryCompositeOperator.OP_NOT;
            }
            default: {
                throw new IllegalArgumentException();
            }
        }
    }

}
