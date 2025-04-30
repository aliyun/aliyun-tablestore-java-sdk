package com.alicloud.openservices.tablestore.model.sql;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

/**
 * Represents an SQL data query request
 */
public class SQLQueryRequest implements Request {

    /**
     * Single row query condition
     */
    private final String query;

    /**
     * Serialization format
     */
    private final SQLPayloadVersion sqlPayloadVersion;

    /**
     * For search pagination query
     */
    private OptionalValue<String> searchToken = new OptionalValue<String>("searchToken");

    public SQLQueryRequest(String query) {
        this(query, SQLPayloadVersion.SQL_FLAT_BUFFERS);
    }

    private SQLQueryRequest(String query, SQLPayloadVersion sqlPayloadVersion) {
        this.query = query;
        this.sqlPayloadVersion = sqlPayloadVersion;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_SQL_Query;
    }

    public String getQuery() {
        return query;
    }

    public SQLPayloadVersion getSqlPayloadVersion() {
        return sqlPayloadVersion;
    }

    public boolean hasSearchToken() {
        return searchToken.isValueSet();
    }

    public String getSearchToken() {
        if (!searchToken.isValueSet()) {
            throw new IllegalStateException("The value of searchToken is not set.");
        }
        return searchToken.getValue();
    }

    public void setSearchToken(String searchToken) {
        this.searchToken.setValue(searchToken);
    }
}
