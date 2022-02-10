package com.alicloud.openservices.tablestore.model.sql;

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

}
