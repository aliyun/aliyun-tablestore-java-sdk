package com.alicloud.openservices.tablestore.model.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Utils method for SQL API
 **/
public class SQLUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SQLUtils.class);

    /**
     * Parse the returned result of Show Tables SQL
     * @param response The returned result of Show Tables SQL
     * @return List of tables
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
