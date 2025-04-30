package com.alicloud.openservices.tablestore.model.sql;

import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Represents the data return collection of an SQL table
 */
public class SQLResultSetImpl implements SQLResultSet {

    private SQLRows sqlRows;

    private int current = 0;

    public SQLResultSetImpl(SQLPayloadVersion version, ByteString rows) {
        this.sqlRows = SQLFactory.getSQLRows(version, rows);
    }

    @Override
    public SQLTableMeta getSQLTableMeta() {
        return sqlRows.getSQLTableMeta();
    }

    @Override
    public boolean hasNext() {
        return current < sqlRows.rowCount();
    }

    @Override
    public SQLRow next() {
        if (hasNext()) {
            SQLRow sqlRow = SQLFactory.getSQLRow(sqlRows, current);
            current++;
            return sqlRow;
        } else {
            throw new IllegalStateException("SQLRow doesn't have next row");
        }
    }

    @Override
    public long rowCount() {
        return sqlRows.rowCount();
    }

    @Override
    public boolean absolute(int rowIndex) {
        if (rowIndex >= sqlRows.rowCount() || rowIndex < 0) {
            return false;
        } else {
            current = rowIndex;
            return true;
        }
    }

}
