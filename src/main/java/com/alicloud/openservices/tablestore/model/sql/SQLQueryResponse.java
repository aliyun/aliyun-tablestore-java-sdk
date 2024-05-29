package com.alicloud.openservices.tablestore.model.sql;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.Response;
import com.google.protobuf.ByteString;

import java.util.Map;

/**
 * 表示 SQL 请求的返回结果
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
     * 获取此次操作消耗的CapacityUnit。
     *
     * @return 此次操作消耗的CapacityUnit。
     */
    public Map<String, ConsumedCapacity> getConsumedCapacity() {
        return consumedCapacityByTable;
    }

    /**
     * 获取 Statement 类型。
     *
     * @return Statement 类型
     */
    public SQLStatementType getSQLStatementType() {
        return type;
    }

    /**
     * 获取 SQL 数据集合。
     * {@link SQLStatementType#SQL_CREATE_TABLE},{@link SQLStatementType#SQL_DROP_TABLE},{@link SQLStatementType#SQL_ALTER_TABLE} 等 DDL 操作返回空
     *
     * @return 数据返回集合
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
