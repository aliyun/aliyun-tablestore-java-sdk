package com.alicloud.openservices.tablestore.model.sql;

import com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.SQLResponseColumns;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

/**
 * Static factory methods related to SQL
 **/
public class SQLFactory {

    public static SQLResultSet getSQLResultSet(SQLPayloadVersion version, SQLStatementType type, ByteString rows) {
        switch (type) {
            case SQL_SELECT:
            case SQL_SHOW_TABLE:
            case SQL_DESCRIBE_TABLE:
                return new SQLResultSetImpl(version, rows);
            case SQL_CREATE_TABLE:
            case SQL_DROP_TABLE:
            case SQL_ALTER_TABLE:
            default:
                return null;
        }
    }

    public static SQLRows getSQLRows(SQLPayloadVersion version, ByteString rows) {
        switch (version) {
            case SQL_FLAT_BUFFERS:
                if (!rows.isEmpty()) {
                    ByteBuffer rowBuffer = rows.asReadOnlyByteBuffer();
                    SQLResponseColumns columns = SQLResponseColumns.getRootAsSQLResponseColumns(rowBuffer);
                    return new SQLRowsFBsColumnBased(columns);
                } else {
                    throw new IllegalStateException("Sql response get rows should not be null");
                }
            default:
                throw new IllegalStateException("Do not support other sql payload version: " + version);
        }
    }

    public static SQLRow getSQLRow(SQLRows sqlRows, int rowIndex) {
        return new SQLRowImpl(sqlRows, rowIndex);
    }

}
