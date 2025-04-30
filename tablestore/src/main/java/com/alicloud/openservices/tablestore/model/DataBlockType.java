package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;

import java.io.IOException;

public enum DataBlockType {
    DBT_PLAIN_BUFFER,
    DBT_SIMPLE_ROW_MATRIX;

    public static DataBlockType fromProtocolType(OtsInternalApi.DataBlockType type) throws IOException {
        switch (type) {
            case DBT_PLAIN_BUFFER:
                return DBT_PLAIN_BUFFER;
            case DBT_SIMPLE_ROW_MATRIX:
                return DBT_SIMPLE_ROW_MATRIX;
            default:
                throw new IOException("Unknown dataBlockType:" + type);
        }
    }
}
