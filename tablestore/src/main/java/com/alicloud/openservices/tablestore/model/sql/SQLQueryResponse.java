package com.alicloud.openservices.tablestore.model.sql;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.Response;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

import java.util.Map;

/**
 * Represents the return result of an SQL request.
 **/
public class SQLQueryResponse extends Response {

    private Map<String, ConsumedCapacity> consumedCapacityByTable;

    private SQLPayloadVersion version;

    private SQLStatementType type;

    private ByteString rows;

    private String nextSearchToken;

    /**
     * internal use
     */
    public SQLQueryResponse(Response meta,
                            Map<String, ConsumedCapacity> consumedCapacityByTable,
                            SQLPayloadVersion version,
                            SQLStatementType type,
                            ByteString rows) {
        super(meta);
        Preconditions.checkNotNull(consumedCapacityByTable);
        this.consumedCapacityByTable = consumedCapacityByTable;
        this.version = version;
        this.type = type;
        this.rows = rows;
    }

    /**
     * Get the CapacityUnit consumed by this operation.
     *
     * @return The CapacityUnit consumed by this operation.
     */
    public Map<String, ConsumedCapacity> getConsumedCapacity() {
        return consumedCapacityByTable;
    }

    /**
     * Get the Statement type.
     *
     * @return Statement type
     */
    public SQLStatementType getSQLStatementType() {
        return type;
    }

    /**
     * Get the SQL data set.
     * DDL operations such as {@link SQLStatementType#SQL_CREATE_TABLE}, {@link SQLStatementType#SQL_DROP_TABLE}, {@link SQLStatementType#SQL_ALTER_TABLE} return empty results.
     *
     * @return Data result set
     */
    public SQLResultSet getSQLResultSet() {
        return SQLFactory.getSQLResultSet(version, type, rows);
    }

    public String getNextSearchToken() {
        return nextSearchToken;
    }

    public void setNextSearchToken(String nextSearchToken) {
        this.nextSearchToken = nextSearchToken;
    }
}
