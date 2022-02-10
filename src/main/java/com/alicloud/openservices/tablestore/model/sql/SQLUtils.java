package com.alicloud.openservices.tablestore.model.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * SQL API 的 Utils 方法
 **/
public class SQLUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SQLUtils.class);

    /**
     * 解析 Show Tables SQL 返回结果
     * @param response Show Tables SQL 返回结果
     * @return 表列表
     */
    public static List<String> parseShowTablesResponse(SQLQueryResponse response) {
        if (response.getSQLStatementType() != SQLStatementType.SQL_SHOW_TABLE) {
            throw new IllegalStateException("SQL statement is not `show tables`.");
        }
        SQLResultSet rs = response.getSQLResultSet();
        List<String> tables = new ArrayList<String>();
        while (rs.hasNext()) {
            tables.add(rs.next().getString(0));
        }
        return tables;
    }

}
