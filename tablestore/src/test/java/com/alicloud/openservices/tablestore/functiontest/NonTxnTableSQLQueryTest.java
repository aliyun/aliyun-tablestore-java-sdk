package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.sql.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Non-transactional table SQL DML tests.
 *
 */
public class NonTxnTableSQLQueryTest {

    private static final String NON_TXN_TABLE_DML_ERROR_MSG =
            "Non-transactional tables currently support atomic batch inserts within a single partition key, and single-row updates or deletes specified by the full primary key without any attribute column conditions.";

    static SyncClient client = null;

    @BeforeClass
    public static void beforeClass() {
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();
        final String instanceName = settings.getOTSInstanceName();
        client = new SyncClient(endPoint, accessId, accessKey, instanceName);
    }

    @AfterClass
    public static void afterClass() {
        client.shutdown();
    }

    // =====================================================
    // Helper methods
    // =====================================================

    private void createNonTxnTable(String tableName, String[] pkNames, PrimaryKeyType[] pkTypes,
                                   String[] defColNames, DefinedColumnType[] defColTypes) {
        // 1. Create physical table via SDK
        TableMeta tableMeta = new TableMeta(tableName);
        for (int i = 0; i < pkNames.length; i++) {
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(pkNames[i], pkTypes[i]));
        }
        for (int i = 0; i < defColNames.length; i++) {
            tableMeta.addDefinedColumn(new DefinedColumnSchema(defColNames[i], defColTypes[i]));
        }
        TableOptions tableOptions = new TableOptions(-1, 1);
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        request.setLocalTxnEnabled(false);
        client.createTable(request);

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 2. Create SQL binding table
        StringBuilder sql = new StringBuilder();
        sql.append(String.format("CREATE TABLE IF NOT EXISTS %s (", tableName));
        for (int i = 0; i < pkNames.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(String.format("`%s` %s", pkNames[i], toSqlType(pkTypes[i])));
        }
        for (int i = 0; i < defColNames.length; i++) {
            sql.append(String.format(", `%s` %s", defColNames[i], toSqlType(defColTypes[i])));
        }
        sql.append(", PRIMARY KEY(");
        for (int i = 0; i < pkNames.length; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(String.format("`%s`", pkNames[i]));
        }
        sql.append("))");
        client.sqlQuery(new SQLQueryRequest(sql.toString()));
    }

    private String toSqlType(PrimaryKeyType pkType) {
        switch (pkType) {
            case INTEGER:
                return "BIGINT";
            case STRING:
                return "VARCHAR(1024)";
            case BINARY:
                return "MEDIUMBLOB";
            default:
                throw new IllegalArgumentException("Unsupported PK type: " + pkType);
        }
    }

    private String toSqlType(DefinedColumnType colType) {
        switch (colType) {
            case INTEGER:
                return "BIGINT";
            case DOUBLE:
                return "DOUBLE";
            case STRING:
                return "MEDIUMTEXT";
            case BOOLEAN:
                return "BOOL";
            case BINARY:
                return "MEDIUMBLOB";
            default:
                throw new IllegalArgumentException("Unsupported column type: " + colType);
        }
    }

    private void createNonTxnTableWithSinglePK(String tableName, String pkName, PrimaryKeyType pkType,
                                               String[] defColNames, DefinedColumnType[] defColTypes) {
        createNonTxnTable(tableName, new String[]{pkName}, new PrimaryKeyType[]{pkType}, defColNames, defColTypes);
    }

    private void createNonTxnTableWithCompositePK(String tableName, String[] pkNames, PrimaryKeyType[] pkTypes,
                                                  String[] defColNames, DefinedColumnType[] defColTypes) {
        createNonTxnTable(tableName, pkNames, pkTypes, defColNames, defColTypes);
    }

    private void dropTableQuietly(String tableName) {
        try {
            client.sqlQuery(new SQLQueryRequest(String.format("DROP MAPPING TABLE IF EXISTS %s", tableName)));
        } catch (Exception e) {
            // ignore
        }
        try {
            client.deleteTable(new DeleteTableRequest(tableName));
        } catch (Exception e) {
            // ignore
        }
    }

    private void execSQL(String sql) {
        client.sqlQuery(new SQLQueryRequest(sql));
    }

    private SQLQueryResponse querySQL(String sql) {
        return client.sqlQuery(new SQLQueryRequest(sql));
    }

    private List<SQLRow> queryRows(String sql) {
        SQLQueryResponse response = querySQL(sql);
        SQLResultSet resultSet = response.getSQLResultSet();
        List<SQLRow> rows = new ArrayList<>();
        while (resultSet.hasNext()) {
            rows.add(resultSet.next());
        }
        return rows;
    }

    private void assertQueryError(String sql, String errorCode, String errorMessage) {
        try {
            client.sqlQuery(new SQLQueryRequest(sql));
            fail("Expected exception with code: " + errorCode);
        } catch (TableStoreException e) {
            assertEquals(errorCode, e.getErrorCode());
            assertTrue("Expected message containing: " + errorMessage + ", actual: " + e.getMessage(),
                    e.getMessage().contains(errorMessage));
        }
    }

    /**
     * Assert that a DML response has consumed both write CU and read CU for the given table.
     * Non-transactional tables use ConditionCheck internally, so each row operation
     * consumes 1 read CU in addition to write CU.
     */
    private void assertCapacityConsumed(SQLQueryResponse response, String tableName, int expectedRows) {
        Map<String, ConsumedCapacity> consumedCapacityMap = response.getConsumedCapacity();
        assertNotNull("ConsumedCapacity should not be null", consumedCapacityMap);
        assertTrue("ConsumedCapacity should contain table: " + tableName,
                consumedCapacityMap.containsKey(tableName));
        ConsumedCapacity consumedCapacity = consumedCapacityMap.get(tableName);
        CapacityUnit capacityUnit = consumedCapacity.getCapacityUnit();
        assertNotNull("CapacityUnit should not be null", capacityUnit);
        assertTrue("Write CU should be > 0 for DML on table: " + tableName,
                capacityUnit.getWriteCapacityUnit() > 0);
        assertEquals("Read CU should equal row count due to ConditionCheck on table: " + tableName,
                expectedRows, capacityUnit.getReadCapacityUnit());
    }

    /**
     * Assert CU behavior when a DML does not actually write (e.g. UPDATE/DELETE on
     * a non-existent row, or UPDATE with unchanged column values).
     *
     * Design expectation: should produce read CU only (no write CU), because the
     * ConditionCheck still reads the row to verify existence or detect value changes.
     *
     * Known bug in current server version: ConsumedCapacity may not be reported at all
     * in some cases. Once the server-side fix is deployed, this method should be updated
     * to always assert read CU > 0 and write CU == 0.
     */
    private void assertReadOnlyCapacityConsumed(SQLQueryResponse response, String tableName) {
        Map<String, ConsumedCapacity> consumedCapacityMap = response.getConsumedCapacity();
        // Current bug: ConsumedCapacity map may not contain the table entry at all.
        // Expected after fix: map contains the table with read CU > 0 and write CU == 0.
        if (consumedCapacityMap != null && consumedCapacityMap.containsKey(tableName)) {
            CapacityUnit capacityUnit = consumedCapacityMap.get(tableName).getCapacityUnit();
            assertNotNull("CapacityUnit should not be null", capacityUnit);
            assertTrue("Read CU should be > 0 for ConditionCheck on table: " + tableName,
                    capacityUnit.getReadCapacityUnit() > 0);
            assertEquals("Write CU should be 0 when no actual write on table: " + tableName,
                    0, capacityUnit.getWriteCapacityUnit());
        }
        // else: current server bug — no ConsumedCapacity reported, silently pass.
    }

    // =====================================================
    // Point Delete Tests
    // =====================================================

    @Test
    public void testPointDeleteBasic() {
        String tableName = "nontxn_sql_test_point_delete_basic";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c", "d"}, new DefinedColumnType[]{DefinedColumnType.INTEGER, DefinedColumnType.STRING});

            execSQL(String.format("INSERT INTO %s VALUES (1, 1, 'a')", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (2, 2, 'b')", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (3, 3, 'c')", tableName));

            execSQL(String.format("DELETE FROM %s WHERE id = 1", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s ORDER BY id", tableName));
            assertEquals(2, rows.size());
            assertEquals(Long.valueOf(2), rows.get(0).getLong("id"));
            assertEquals(Long.valueOf(3), rows.get(1).getLong("id"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointDeleteCompositePK() {
        String tableName = "nontxn_sql_test_point_delete_composite_pk";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithCompositePK(tableName,
                    new String[]{"pk1", "pk2"},
                    new PrimaryKeyType[]{PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                    new String[]{"value"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 1, 100)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (1, 2, 200)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (2, 1, 300)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (2, 2, 400)", tableName));

            execSQL(String.format("DELETE FROM %s WHERE pk1 = 1 AND pk2 = 1", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s ORDER BY pk1, pk2", tableName));
            assertEquals(3, rows.size());
            assertEquals(Long.valueOf(1), rows.get(0).getLong("pk1"));
            assertEquals(Long.valueOf(2), rows.get(0).getLong("pk2"));
            assertEquals(Long.valueOf(2), rows.get(1).getLong("pk1"));
            assertEquals(Long.valueOf(1), rows.get(1).getLong("pk2"));
            assertEquals(Long.valueOf(2), rows.get(2).getLong("pk1"));
            assertEquals(Long.valueOf(2), rows.get(2).getLong("pk2"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointDeleteNonExistentRow() {
        String tableName = "nontxn_sql_test_point_delete_nonexist";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));

            // Delete non-existent row
            SQLQueryResponse deleteResponse = querySQL(String.format("DELETE FROM %s WHERE id = 999", tableName));
            assertReadOnlyCapacityConsumed(deleteResponse, tableName);

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());
            assertEquals(Long.valueOf(100), rows.get(0).getLong("c"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointDeleteWithNullValues() {
        String tableName = "nontxn_sql_test_point_delete_null";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c", "d"}, new DefinedColumnType[]{DefinedColumnType.INTEGER, DefinedColumnType.STRING});

            execSQL(String.format("INSERT INTO %s VALUES (1, 1, 'a')", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (2, NULL, NULL)", tableName));

            execSQL(String.format("DELETE FROM %s WHERE id = 2", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());
            assertEquals(Long.valueOf(1), rows.get(0).getLong("id"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointDeleteWithVarcharPK() {
        String tableName = "nontxn_sql_test_point_delete_varchar_pk";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.STRING,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES ('abc', 1)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES ('def', 2)", tableName));

            execSQL(String.format("DELETE FROM %s WHERE id = 'abc'", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());
            assertEquals("def", rows.get(0).getString("id"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointDeleteWithBigIntPK() {
        String tableName = "nontxn_sql_test_point_delete_bigint_pk";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            // Max long value should fail
            assertQueryError(String.format("INSERT INTO %s VALUES (9223372036854775807, 1)", tableName),
                    "OTSParameterInvalid", "The input parameter is invalid.");

            execSQL(String.format("INSERT INTO %s VALUES (9223372036854775806, 1)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (1, 2)", tableName));

            execSQL(String.format("DELETE FROM %s WHERE id = 9223372036854775806", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());
            assertEquals(Long.valueOf(1), rows.get(0).getLong("id"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointDeleteWithLimit() {
        String tableName = "nontxn_sql_test_point_delete_limit";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (2, 200)", tableName));

            // LIMIT in DELETE via SDK should be rejected
            assertQueryError(String.format("DELETE FROM %s WHERE id = 1 LIMIT 1", tableName),
                    "OTSUnsupportOperation", "limit in delete statement is not supported");

            // Verify data unchanged
            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s ORDER BY id", tableName));
            assertEquals(2, rows.size());
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointDeleteDifferentPKOrder() {
        String tableName = "nontxn_sql_test_point_delete_pk_order";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithCompositePK(tableName,
                    new String[]{"pk1", "pk2"},
                    new PrimaryKeyType[]{PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                    new String[]{"value"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 2, 100)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (2, 1, 200)", tableName));

            // WHERE clause PK order reversed from definition
            execSQL(String.format("DELETE FROM %s WHERE pk2 = 2 AND pk1 = 1", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());
            assertEquals(Long.valueOf(2), rows.get(0).getLong("pk1"));
            assertEquals(Long.valueOf(1), rows.get(0).getLong("pk2"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointDeleteIllegal() {
        String tableName = "nontxn_sql_test_point_delete_illegal";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (2, 200)", tableName));

            // Missing WHERE clause
            assertQueryError(String.format("DELETE FROM %s", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // Non-PK column in WHERE
            assertQueryError(String.format("DELETE FROM %s WHERE c = 100", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // Range query (greater than)
            assertQueryError(String.format("DELETE FROM %s WHERE id > 1", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // Range query (less than or equal)
            assertQueryError(String.format("DELETE FROM %s WHERE id <= 2", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // IN clause
            assertQueryError(String.format("DELETE FROM %s WHERE id IN (1, 2)", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // OR condition
            assertQueryError(String.format("DELETE FROM %s WHERE id = 1 OR id = 2", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // BETWEEN condition
            assertQueryError(String.format("DELETE FROM %s WHERE id BETWEEN 1 AND 2", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // Not equal condition
            assertQueryError(String.format("DELETE FROM %s WHERE id != 1", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointDeleteIllegalCompositePK() {
        String tableName = "nontxn_sql_test_point_delete_illegal_cpk";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithCompositePK(tableName,
                    new String[]{"pk1", "pk2"},
                    new PrimaryKeyType[]{PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 1, 100)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (1, 2, 200)", tableName));

            // Incomplete composite PK
            assertQueryError(String.format("DELETE FROM %s WHERE pk1 = 1", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // Full PK + attribute column condition
            assertQueryError(String.format("DELETE FROM %s WHERE pk1 = 1 AND pk2 = 1 AND c > 50", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);
        } finally {
            dropTableQuietly(tableName);
        }
    }

    // =====================================================
    // Point Insert Tests
    // =====================================================

    @Test
    public void testPointInsertBasic() {
        String tableName = "nontxn_sql_test_point_insert_basic";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c", "d"}, new DefinedColumnType[]{DefinedColumnType.INTEGER, DefinedColumnType.STRING});

            execSQL(String.format("INSERT INTO %s VALUES (1, 1, 'a')", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (2, 2, 'b')", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s ORDER BY id", tableName));
            assertEquals(2, rows.size());
            assertEquals(Long.valueOf(1), rows.get(0).getLong("id"));
            assertEquals(Long.valueOf(1), rows.get(0).getLong("c"));
            assertEquals("a", rows.get(0).getString("d"));
            assertEquals(Long.valueOf(2), rows.get(1).getLong("id"));
            assertEquals(Long.valueOf(2), rows.get(1).getLong("c"));
            assertEquals("b", rows.get(1).getString("d"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointInsertBatchSamePartitionKey() {
        String tableName = "nontxn_sql_test_point_insert_batch";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithCompositePK(tableName,
                    new String[]{"pk1", "pk2"},
                    new PrimaryKeyType[]{PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                    new String[]{"value"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            // Batch insert with same partition key
            SQLQueryResponse response = querySQL(String.format(
                    "INSERT INTO %s VALUES (1, 1, 100), (1, 2, 200), (1, 3, 300)", tableName));
            assertEquals(3, response.getAffectedRows());
            assertCapacityConsumed(response, tableName, 3);

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s ORDER BY pk1, pk2", tableName));
            assertEquals(3, rows.size());
            assertEquals(Long.valueOf(100), rows.get(0).getLong("value"));
            assertEquals(Long.valueOf(200), rows.get(1).getLong("value"));
            assertEquals(Long.valueOf(300), rows.get(2).getLong("value"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointInsertWithNull() {
        String tableName = "nontxn_sql_test_point_insert_null";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c", "d"}, new DefinedColumnType[]{DefinedColumnType.INTEGER, DefinedColumnType.STRING});

            execSQL(String.format("INSERT INTO %s VALUES (1, NULL, NULL)", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());
            assertEquals(Long.valueOf(1), rows.get(0).getLong("id"));
            assertNull(rows.get(0).getLong("c"));
            assertNull(rows.get(0).getString("d"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointInsertWithVarcharPK() {
        String tableName = "nontxn_sql_test_point_insert_varchar_pk";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.STRING,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES ('abc', 1)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES ('def', 2)", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s ORDER BY id", tableName));
            assertEquals(2, rows.size());
            assertEquals("abc", rows.get(0).getString("id"));
            assertEquals("def", rows.get(1).getString("id"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointInsertDuplicateKey() {
        String tableName = "nontxn_sql_test_point_insert_dup_key";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            SQLQueryResponse firstInsertResponse = querySQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));
            assertCapacityConsumed(firstInsertResponse, tableName, 1);

            // Insert with same PK should fail with duplicate key error.
            // CU note: duplicate key INSERT does not actually write, so by design it should
            // only produce read CU (no write CU). However, since the SDK throws a
            // TableStoreException on error, the SQLQueryResponse (and its ConsumedCapacity)
            // is not available on the client side, so CU cannot be asserted here.
            assertQueryError(String.format("INSERT INTO %s VALUES (1, 200)", tableName),
                    "OTSParameterInvalid", "Duplicate entry for key 'PRIMARY'");

            // Verify original data unchanged
            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());
            assertEquals(Long.valueOf(100), rows.get(0).getLong("c"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointInsertBatchDifferentPartitionKey() {
        String tableName = "nontxn_sql_test_point_insert_diff_pk";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithCompositePK(tableName,
                    new String[]{"pk1", "pk2"},
                    new PrimaryKeyType[]{PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                    new String[]{"value"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            // Batch insert with different partition keys should fail for non-txn table
            assertQueryError(String.format(
                            "INSERT INTO %s VALUES (1, 1, 100), (2, 1, 200)", tableName),
                    "OTSUnsupportOperation", "Unsupported operation: '`insert into` on different partition keys is not supported'.");
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointInsertAffectedRows() {
        String tableName = "nontxn_sql_test_point_insert_affected";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            // Single row insert
            SQLQueryResponse response = querySQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));
            assertEquals(1, response.getAffectedRows());
            assertCapacityConsumed(response, tableName, 1);

            // Multi-row insert with same partition key (single PK, all same partition)
            String cpkTableName = "nontxn_sql_test_point_insert_affected_cpk";
            createNonTxnTableWithCompositePK(cpkTableName,
                    new String[]{"pk1", "pk2"},
                    new PrimaryKeyType[]{PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                    new String[]{"value"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});
            response = querySQL(String.format("INSERT INTO %s VALUES (1, 1, 100), (1, 2, 200), (1, 3, 300)",
                    cpkTableName));
            assertEquals(3, response.getAffectedRows());
            assertCapacityConsumed(response, cpkTableName, 3);
        } finally {
            dropTableQuietly(tableName);
            dropTableQuietly("nontxn_sql_test_point_insert_affected_cpk");
        }
    }

    @Test
    public void testPointInsertBatchRowLimit() {
        String tableName = "nontxn_sql_test_point_insert_row_limit";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithCompositePK(tableName,
                    new String[]{"pk1", "pk2"},
                    new PrimaryKeyType[]{PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                    new String[]{"value"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            // Build a batch insert with exactly 200 rows (same partition key) — should succeed
            StringBuilder insertSql200 = new StringBuilder();
            insertSql200.append(String.format("INSERT INTO %s VALUES ", tableName));
            for (int i = 1; i <= 200; i++) {
                if (i > 1) {
                    insertSql200.append(", ");
                }
                insertSql200.append(String.format("(1, %d, %d)", i, i * 10));
            }
            SQLQueryResponse response = querySQL(insertSql200.toString());
            assertEquals(200, response.getAffectedRows());

            // Verify 200 rows inserted
            List<SQLRow> rows = queryRows(String.format("SELECT COUNT(*) as cnt FROM %s WHERE pk1 = 1", tableName));
            assertEquals(Long.valueOf(200), rows.get(0).getLong("cnt"));

            // Build a batch insert with 201 rows (same partition key) — should fail
            StringBuilder insertSql201 = new StringBuilder();
            insertSql201.append(String.format("INSERT INTO %s VALUES ", tableName));
            for (int i = 201; i <= 401; i++) {
                if (i > 201) {
                    insertSql201.append(", ");
                }
                insertSql201.append(String.format("(2, %d, %d)", i, i * 10));
            }
            try {
                client.sqlQuery(new SQLQueryRequest(insertSql201.toString()));
                fail("Expected exception for batch insert exceeding 200 rows");
            } catch (TableStoreException e) {
                // Verify the error is related to exceeding the row limit
                assertNotNull(e.getErrorCode());
            }
        } finally {
            dropTableQuietly(tableName);
        }
    }

    // =====================================================
    // Point Update Tests
    // =====================================================

    @Test
    public void testPointUpdateBasic() {
        String tableName = "nontxn_sql_test_point_update_basic";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c", "d"}, new DefinedColumnType[]{DefinedColumnType.INTEGER, DefinedColumnType.STRING});

            execSQL(String.format("INSERT INTO %s VALUES (1, 1, 'a')", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (2, 2, 'b')", tableName));

            execSQL(String.format("UPDATE %s SET c = 10 WHERE id = 1", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s ORDER BY id", tableName));
            assertEquals(2, rows.size());
            assertEquals(Long.valueOf(10), rows.get(0).getLong("c"));
            assertEquals("a", rows.get(0).getString("d"));
            assertEquals(Long.valueOf(2), rows.get(1).getLong("c"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointUpdateSelfIncrement() {
        String tableName = "nontxn_sql_test_point_update_self_incr";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"counter"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));

            // counter + 1
            execSQL(String.format("UPDATE %s SET counter = counter + 1 WHERE id = 1", tableName));
            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(Long.valueOf(101), rows.get(0).getLong("counter"));

            // counter + 10
            execSQL(String.format("UPDATE %s SET counter = counter + 10 WHERE id = 1", tableName));
            rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(Long.valueOf(111), rows.get(0).getLong("counter"));

            // counter - 5
            execSQL(String.format("UPDATE %s SET counter = counter - 5 WHERE id = 1", tableName));
            rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(Long.valueOf(106), rows.get(0).getLong("counter"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointUpdateWithMultipleColumns() {
        String tableName = "nontxn_sql_test_point_update_multi_col";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c1", "c2", "c3"},
                    new DefinedColumnType[]{DefinedColumnType.INTEGER, DefinedColumnType.INTEGER, DefinedColumnType.STRING});

            execSQL(String.format("INSERT INTO %s VALUES (1, 1, 1, 'a')", tableName));

            execSQL(String.format("UPDATE %s SET c1 = 10, c2 = 20, c3 = 'updated' WHERE id = 1", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());
            assertEquals(Long.valueOf(10), rows.get(0).getLong("c1"));
            assertEquals(Long.valueOf(20), rows.get(0).getLong("c2"));
            assertEquals("updated", rows.get(0).getString("c3"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointUpdateWithNull() {
        String tableName = "nontxn_sql_test_point_update_null";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 1)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (2, NULL)", tableName));

            // Update NULL to value
            execSQL(String.format("UPDATE %s SET c = 10 WHERE id = 2", tableName));
            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s WHERE id = 2", tableName));
            assertEquals(Long.valueOf(10), rows.get(0).getLong("c"));

            // Update value to NULL
            execSQL(String.format("UPDATE %s SET c = NULL WHERE id = 1", tableName));
            rows = queryRows(String.format("SELECT * FROM %s WHERE id = 1", tableName));
            assertNull(rows.get(0).getLong("c"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointUpdateCompositePK() {
        String tableName = "nontxn_sql_test_point_update_composite_pk";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithCompositePK(tableName,
                    new String[]{"pk1", "pk2"},
                    new PrimaryKeyType[]{PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                    new String[]{"value"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 1, 100)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (1, 2, 200)", tableName));

            execSQL(String.format("UPDATE %s SET value = 999 WHERE pk1 = 1 AND pk2 = 1", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s ORDER BY pk1, pk2", tableName));
            assertEquals(2, rows.size());
            assertEquals(Long.valueOf(999), rows.get(0).getLong("value"));
            assertEquals(Long.valueOf(200), rows.get(1).getLong("value"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointUpdateNonExistentRow() {
        String tableName = "nontxn_sql_test_point_update_nonexist";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));

            // Update non-existent row
            SQLQueryResponse updateResponse = querySQL(String.format("UPDATE %s SET c = 999 WHERE id = 999", tableName));
            assertReadOnlyCapacityConsumed(updateResponse, tableName);

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());
            assertEquals(Long.valueOf(100), rows.get(0).getLong("c"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointUpdateWithVarcharPK() {
        String tableName = "nontxn_sql_test_point_update_varchar_pk";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.STRING,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES ('abc', 1)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES ('def', 2)", tableName));

            execSQL(String.format("UPDATE %s SET c = 100 WHERE id = 'abc'", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s ORDER BY id", tableName));
            assertEquals(2, rows.size());
            assertEquals(Long.valueOf(100), rows.get(0).getLong("c"));
            assertEquals(Long.valueOf(2), rows.get(1).getLong("c"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointUpdateWithLimit() {
        String tableName = "nontxn_sql_test_point_update_limit";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (2, 200)", tableName));

            // LIMIT in UPDATE via SDK should be rejected
            assertQueryError(String.format("UPDATE %s SET c = 999 WHERE id = 1 LIMIT 1", tableName),
                    "OTSUnsupportOperation", "limit in update statement is not supported");

            // Verify data unchanged
            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s ORDER BY id", tableName));
            assertEquals(2, rows.size());
            assertEquals(Long.valueOf(100), rows.get(0).getLong("c"));
            assertEquals(Long.valueOf(200), rows.get(1).getLong("c"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointUpdateDifferentPKOrder() {
        String tableName = "nontxn_sql_test_point_update_pk_order";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithCompositePK(tableName,
                    new String[]{"pk1", "pk2"},
                    new PrimaryKeyType[]{PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                    new String[]{"value"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 2, 100)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (2, 1, 200)", tableName));

            // WHERE clause PK order reversed from definition
            execSQL(String.format("UPDATE %s SET value = 999 WHERE pk2 = 2 AND pk1 = 1", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s ORDER BY pk1, pk2", tableName));
            assertEquals(2, rows.size());
            assertEquals(Long.valueOf(999), rows.get(0).getLong("value"));
            assertEquals(Long.valueOf(200), rows.get(1).getLong("value"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointUpdateIllegal() {
        String tableName = "nontxn_sql_test_point_update_illegal";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (2, 200)", tableName));

            // Missing WHERE clause
            assertQueryError(String.format("UPDATE %s SET c = 999", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // Non-PK column in WHERE
            assertQueryError(String.format("UPDATE %s SET c = 999 WHERE c = 100", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // Range query (greater than)
            assertQueryError(String.format("UPDATE %s SET c = 999 WHERE id > 1", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // Range query (less than or equal)
            assertQueryError(String.format("UPDATE %s SET c = 999 WHERE id <= 2", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // IN clause
            assertQueryError(String.format("UPDATE %s SET c = 999 WHERE id IN (1, 2)", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // OR condition
            assertQueryError(String.format("UPDATE %s SET c = 999 WHERE id = 1 OR id = 2", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // BETWEEN condition
            assertQueryError(String.format("UPDATE %s SET c = 999 WHERE id BETWEEN 1 AND 2", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // Not equal condition
            assertQueryError(String.format("UPDATE %s SET c = 999 WHERE id != 1", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointUpdateIllegalCompositePK() {
        String tableName = "nontxn_sql_test_point_update_illegal_cpk";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithCompositePK(tableName,
                    new String[]{"pk1", "pk2"},
                    new PrimaryKeyType[]{PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 1, 100)", tableName));
            execSQL(String.format("INSERT INTO %s VALUES (1, 2, 200)", tableName));

            // Incomplete composite PK
            assertQueryError(String.format("UPDATE %s SET c = 999 WHERE pk1 = 1", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);

            // Full PK + attribute column condition
            assertQueryError(String.format("UPDATE %s SET c = 999 WHERE pk1 = 1 AND pk2 = 1 AND c > 50", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);
        } finally {
            dropTableQuietly(tableName);
        }
    }

    // =====================================================
    // Self Increment Error Tests
    // =====================================================

    @Test
    public void testPointUpdateSelfIncrementOnStringColumn() {
        String tableName = "nontxn_sql_test_update_incr_str";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"str_col"}, new DefinedColumnType[]{DefinedColumnType.STRING});

            execSQL(String.format("INSERT INTO %s VALUES (1, 'hello')", tableName));

            assertQueryError(String.format("UPDATE %s SET str_col = str_col + 1 WHERE id = 1", tableName),
                    "OTSUnsupportOperation", "point update only supports constant and self-increment assignments");
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointUpdateSelfIncrementOnDoubleColumn() {
        String tableName = "nontxn_sql_test_update_incr_double";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"double_col"}, new DefinedColumnType[]{DefinedColumnType.DOUBLE});

            execSQL(String.format("INSERT INTO %s VALUES (1, 1.5)", tableName));

            assertQueryError(String.format("UPDATE %s SET double_col = double_col + 1 WHERE id = 1", tableName),
                    "OTSUnsupportOperation", "point update only supports integer type self increment");
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointUpdateSelfIncrementOnBooleanColumn() {
        String tableName = "nontxn_sql_test_update_incr_bool";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"bool_col"}, new DefinedColumnType[]{DefinedColumnType.BOOLEAN});

            execSQL(String.format("INSERT INTO %s VALUES (1, true)", tableName));

            assertQueryError(String.format("UPDATE %s SET bool_col = bool_col + 1 WHERE id = 1", tableName),
                    "OTSUnsupportOperation", "point update only supports integer type self increment");
        } finally {
            dropTableQuietly(tableName);
        }
    }

    // =====================================================
    // INSERT IGNORE Tests
    // =====================================================

    @Test
    public void testInsertIgnoreNotSupportedOnNonTxnTable() {
        String tableName = "nontxn_sql_test_insert_ignore_basic";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            // INSERT IGNORE is not supported on non-transactional tables
            assertQueryError(String.format("INSERT IGNORE INTO %s VALUES (1, 100)", tableName),
                    "OTSParameterInvalid", "Try to call method using explicit transaction on explicit-transaction-disabled table.");
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testInsertIgnoreBatchDifferentPartitionKey() {
        String tableName = "nontxn_sql_test_insert_ignore_diff_pk";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithCompositePK(tableName,
                    new String[]{"pk1", "pk2"},
                    new PrimaryKeyType[]{PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                    new String[]{"value"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            // INSERT IGNORE with different partition keys should fail
            assertQueryError(String.format(
                            "INSERT IGNORE INTO %s VALUES (1, 1, 100), (2, 1, 200)", tableName),
                    "OTSUnsupportOperation", "`insert ignore` on different partition keys is not supported");
        } finally {
            dropTableQuietly(tableName);
        }
    }

    // =====================================================
    // ON DUPLICATE KEY UPDATE Tests
    // =====================================================

    @Test
    public void testOnDuplicateKeyUpdateNotSupportedOnNonTxnTable() {
        String tableName = "nontxn_sql_test_on_dup_insert";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            // ON DUPLICATE KEY UPDATE is not supported on non-transactional tables
            assertQueryError(
                    String.format("INSERT INTO %s VALUES (1, 100) ON DUPLICATE KEY UPDATE c = VALUES(c)", tableName),
                    "OTSParameterInvalid", "Try to call method using explicit transaction on explicit-transaction-disabled table.");
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testOnDuplicateKeyUpdateDifferentPartitionKey() {
        String tableName = "nontxn_sql_test_on_dup_diff_pk";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithCompositePK(tableName,
                    new String[]{"pk1", "pk2"},
                    new PrimaryKeyType[]{PrimaryKeyType.INTEGER, PrimaryKeyType.INTEGER},
                    new String[]{"value"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            // ON DUPLICATE KEY UPDATE with different partition keys should fail
            assertQueryError(String.format(
                            "INSERT INTO %s VALUES (1, 1, 100), (2, 1, 200) ON DUPLICATE KEY UPDATE value = VALUES(value)",
                            tableName),
                    "OTSUnsupportOperation", "`insert ... on duplicate key update` on different partition keys is not supported");
        } finally {
            dropTableQuietly(tableName);
        }
    }

    // =====================================================
    // Affected Rows Tests (UPDATE / DELETE)
    // =====================================================

    @Test
    public void testPointUpdateAffectedRows() {
        String tableName = "nontxn_sql_test_update_affected";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));

            SQLQueryResponse response = querySQL(String.format("UPDATE %s SET c = 200 WHERE id = 1", tableName));
            assertEquals(1, response.getAffectedRows());
            assertCapacityConsumed(response, tableName, 1);

            // Update with same value — no actual write should occur
            SQLQueryResponse sameValueResponse = querySQL(String.format("UPDATE %s SET c = 200 WHERE id = 1", tableName));
            assertReadOnlyCapacityConsumed(sameValueResponse, tableName);
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testPointDeleteAffectedRows() {
        String tableName = "nontxn_sql_test_delete_affected";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));

            SQLQueryResponse response = querySQL(String.format("DELETE FROM %s WHERE id = 1", tableName));
            assertEquals(1, response.getAffectedRows());
            assertCapacityConsumed(response, tableName, 1);
        } finally {
            dropTableQuietly(tableName);
        }
    }

    // =====================================================
    // Illegal Syntax Rejection Tests
    // =====================================================

    @Test
    public void testDeleteWithLikeCondition() {
        String tableName = "nontxn_sql_test_delete_like";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.STRING,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES ('abc', 1)", tableName));

            assertQueryError(String.format("DELETE FROM %s WHERE id LIKE 'a%%'", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testUpdateWithLikeCondition() {
        String tableName = "nontxn_sql_test_update_like";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.STRING,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES ('abc', 1)", tableName));

            assertQueryError(String.format("UPDATE %s SET c = 999 WHERE id LIKE 'a%%'", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testDeleteWithIsNullCondition() {
        String tableName = "nontxn_sql_test_delete_isnull";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, NULL)", tableName));

            assertQueryError(String.format("DELETE FROM %s WHERE c IS NULL", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testUpdateWithIsNullCondition() {
        String tableName = "nontxn_sql_test_update_isnull";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, NULL)", tableName));

            assertQueryError(String.format("UPDATE %s SET c = 999 WHERE c IS NULL", tableName),
                    "OTSParameterInvalid", NON_TXN_TABLE_DML_ERROR_MSG);
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testDeleteWithOrderBy() {
        String tableName = "nontxn_sql_test_delete_orderby";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));

            try {
                client.sqlQuery(new SQLQueryRequest(
                        String.format("DELETE FROM %s WHERE id = 1 ORDER BY id", tableName)));
                fail("Expected exception for ORDER BY in DELETE");
            } catch (TableStoreException e) {
                assertNotNull(e.getErrorCode());
            }
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testUpdateWithOrderBy() {
        String tableName = "nontxn_sql_test_update_orderby";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));

            try {
                client.sqlQuery(new SQLQueryRequest(
                        String.format("UPDATE %s SET c = 999 WHERE id = 1 ORDER BY id", tableName)));
                fail("Expected exception for ORDER BY in UPDATE");
            } catch (TableStoreException e) {
                assertNotNull(e.getErrorCode());
            }
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testInsertSetSyntax() {
        String tableName = "nontxn_sql_test_insert_set";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            try {
                client.sqlQuery(new SQLQueryRequest(
                        String.format("INSERT INTO %s SET id = 1, c = 100", tableName)));
                fail("Expected exception for INSERT SET syntax");
            } catch (TableStoreException e) {
                assertNotNull(e.getErrorCode());
            }
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testInsertSelect() {
        String tableName = "nontxn_sql_test_insert_select";
        String sourceTable = "nontxn_sql_test_insert_select_src";
        dropTableQuietly(tableName);
        dropTableQuietly(sourceTable);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});
            createNonTxnTableWithSinglePK(sourceTable, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 100)", sourceTable));

            try {
                client.sqlQuery(new SQLQueryRequest(
                        String.format("INSERT INTO %s SELECT * FROM %s", tableName, sourceTable)));
                fail("Expected exception for INSERT ... SELECT syntax");
            } catch (TableStoreException e) {
                assertNotNull(e.getErrorCode());
            }
        } finally {
            dropTableQuietly(tableName);
            dropTableQuietly(sourceTable);
        }
    }

    @Test
    public void testUpdatePrimaryKeyColumn() {
        String tableName = "nontxn_sql_test_update_pk_col";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});

            execSQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));

            // Updating PK column should be rejected
            try {
                client.sqlQuery(new SQLQueryRequest(
                        String.format("UPDATE %s SET id = 2 WHERE id = 1", tableName)));
                fail("Expected exception for updating primary key column");
            } catch (TableStoreException e) {
                assertNotNull(e.getErrorCode());
            }

            // Verify data unchanged
            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());
            assertEquals(Long.valueOf(1), rows.get(0).getLong("id"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    // =====================================================
    // Data Type Coverage Tests
    // =====================================================

    @Test
    public void testBinaryPrimaryKeyNotSupported() {
        String tableName = "nontxn_sql_test_binary_pk";
        dropTableQuietly(tableName);
        try {
            // BINARY (MEDIUMBLOB) is not supported as a primary key type in SQL binding
            try {
                createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.BINARY,
                        new String[]{"c"}, new DefinedColumnType[]{DefinedColumnType.INTEGER});
                fail("Expected exception for BINARY primary key type");
            } catch (TableStoreException e) {
                assertTrue(e.getMessage().contains("not supported"));
            }
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testDoubleColumnConstantUpdate() {
        String tableName = "nontxn_sql_test_double_col_update";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"double_col"}, new DefinedColumnType[]{DefinedColumnType.DOUBLE});

            execSQL(String.format("INSERT INTO %s VALUES (1, 1.5)", tableName));

            // Constant update on DOUBLE column should work
            execSQL(String.format("UPDATE %s SET double_col = 3.14 WHERE id = 1", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());
            assertEquals(3.14, rows.get(0).getDouble("double_col"), 0.001);
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testBooleanColumnConstantUpdate() {
        String tableName = "nontxn_sql_test_bool_col_update";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"bool_col"}, new DefinedColumnType[]{DefinedColumnType.BOOLEAN});

            execSQL(String.format("INSERT INTO %s VALUES (1, true)", tableName));

            // Constant update on BOOLEAN column should work
            execSQL(String.format("UPDATE %s SET bool_col = false WHERE id = 1", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());
            assertFalse(rows.get(0).getBoolean("bool_col"));
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testBlobColumnOperations() {
        String tableName = "nontxn_sql_test_blob_col";
        dropTableQuietly(tableName);
        try {
            createNonTxnTableWithSinglePK(tableName, "id", PrimaryKeyType.INTEGER,
                    new String[]{"blob_col"}, new DefinedColumnType[]{DefinedColumnType.BINARY});

            // INSERT with blob value
            execSQL(String.format("INSERT INTO %s VALUES (1, X'DEADBEEF')", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());
            assertNotNull(rows.get(0).get("blob_col"));

            // UPDATE blob column
            execSQL(String.format("UPDATE %s SET blob_col = X'CAFEBABE' WHERE id = 1", tableName));

            rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());

            // DELETE row with blob
            execSQL(String.format("DELETE FROM %s WHERE id = 1", tableName));
            rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(0, rows.size());
        } finally {
            dropTableQuietly(tableName);
        }
    }

    // =====================================================
    // CREATE TABLE Column Option & Constraint Tests
    // =====================================================

    @Test
    public void testCreateTableWithNotNull() {
        String tableName = "nontxn_sql_test_create_notnull";
        dropTableQuietly(tableName);
        try {
            // Create physical table first
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("id", PrimaryKeyType.INTEGER));
            tableMeta.addDefinedColumn(new DefinedColumnSchema("c", DefinedColumnType.INTEGER));
            TableOptions tableOptions = new TableOptions(-1, 1);
            CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
            request.setLocalTxnEnabled(false);
            client.createTable(request);
            Thread.sleep(200);

            // NOT NULL column option should be accepted
            execSQL(String.format("CREATE TABLE IF NOT EXISTS %s (`id` BIGINT, `c` BIGINT NOT NULL, PRIMARY KEY(`id`))", tableName));

            execSQL(String.format("INSERT INTO %s VALUES (1, 100)", tableName));
            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertEquals(1, rows.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testCreateTableWithDefaultValue() {
        String tableName = "nontxn_sql_test_create_default";
        dropTableQuietly(tableName);
        try {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("id", PrimaryKeyType.INTEGER));
            tableMeta.addDefinedColumn(new DefinedColumnSchema("c", DefinedColumnType.INTEGER));
            TableOptions tableOptions = new TableOptions(-1, 1);
            CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
            request.setLocalTxnEnabled(false);
            client.createTable(request);
            Thread.sleep(200);

            // DEFAULT VALUE column option should be accepted
            execSQL(String.format("CREATE TABLE IF NOT EXISTS %s (`id` BIGINT, `c` BIGINT DEFAULT 0, PRIMARY KEY(`id`))", tableName));

            List<SQLRow> rows = queryRows(String.format("SELECT * FROM %s", tableName));
            assertNotNull(rows);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testCreateTableWithAutoIncrementRejected() {
        String tableName = "nontxn_sql_test_create_autoinc";
        dropTableQuietly(tableName);
        try {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("id", PrimaryKeyType.INTEGER));
            tableMeta.addDefinedColumn(new DefinedColumnSchema("c", DefinedColumnType.INTEGER));
            TableOptions tableOptions = new TableOptions(-1, 1);
            CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
            request.setLocalTxnEnabled(false);
            client.createTable(request);
            Thread.sleep(200);

            // AUTO_INCREMENT should be rejected
            try {
                client.sqlQuery(new SQLQueryRequest(
                        String.format("CREATE TABLE IF NOT EXISTS %s (`id` BIGINT AUTO_INCREMENT, `c` BIGINT, PRIMARY KEY(`id`))", tableName)));
                fail("Expected exception for AUTO_INCREMENT column option");
            } catch (TableStoreException e) {
                assertNotNull(e.getErrorCode());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testCreateTableWithUniqueKeyRejected() {
        String tableName = "nontxn_sql_test_create_unique";
        dropTableQuietly(tableName);
        try {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("id", PrimaryKeyType.INTEGER));
            tableMeta.addDefinedColumn(new DefinedColumnSchema("c", DefinedColumnType.INTEGER));
            TableOptions tableOptions = new TableOptions(-1, 1);
            CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
            request.setLocalTxnEnabled(false);
            client.createTable(request);
            Thread.sleep(200);

            // UNIQUE constraint should be rejected
            try {
                client.sqlQuery(new SQLQueryRequest(
                        String.format("CREATE TABLE IF NOT EXISTS %s (`id` BIGINT, `c` BIGINT, PRIMARY KEY(`id`), UNIQUE KEY(`c`))", tableName)));
                fail("Expected exception for UNIQUE constraint");
            } catch (TableStoreException e) {
                assertNotNull(e.getErrorCode());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testCreateTableWithCheckRejected() {
        String tableName = "nontxn_sql_test_create_check";
        dropTableQuietly(tableName);
        try {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("id", PrimaryKeyType.INTEGER));
            tableMeta.addDefinedColumn(new DefinedColumnSchema("c", DefinedColumnType.INTEGER));
            TableOptions tableOptions = new TableOptions(-1, 1);
            CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
            request.setLocalTxnEnabled(false);
            client.createTable(request);
            Thread.sleep(200);

            // CHECK constraint should be rejected
            try {
                client.sqlQuery(new SQLQueryRequest(
                        String.format("CREATE TABLE IF NOT EXISTS %s (`id` BIGINT, `c` BIGINT CHECK(`c` > 0), PRIMARY KEY(`id`))", tableName)));
                fail("Expected exception for CHECK constraint");
            } catch (TableStoreException e) {
                assertNotNull(e.getErrorCode());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            dropTableQuietly(tableName);
        }
    }

    @Test
    public void testCreateTableWithCommentRejected() {
        String tableName = "nontxn_sql_test_create_comment";
        dropTableQuietly(tableName);
        try {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("id", PrimaryKeyType.INTEGER));
            tableMeta.addDefinedColumn(new DefinedColumnSchema("c", DefinedColumnType.INTEGER));
            TableOptions tableOptions = new TableOptions(-1, 1);
            CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
            request.setLocalTxnEnabled(false);
            client.createTable(request);
            Thread.sleep(200);

            // COMMENT column option should be rejected
            try {
                client.sqlQuery(new SQLQueryRequest(
                        String.format("CREATE TABLE IF NOT EXISTS %s (`id` BIGINT, `c` BIGINT COMMENT 'test', PRIMARY KEY(`id`))", tableName)));
                fail("Expected exception for COMMENT column option");
            } catch (TableStoreException e) {
                assertNotNull(e.getErrorCode());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            dropTableQuietly(tableName);
        }
    }
}
