package com.alicloud.openservices.tablestore.model.sql;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

/**
 * 表示 SQL 数据查询请求
 */
public class SQLQueryRequest implements Request {

    /**
     * 单行查询条件
     */
    private final String query;

    /**
     * 序列化格式
     */
    private final SQLPayloadVersion sqlPayloadVersion;

    /**
     * 用于search翻页查询
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
