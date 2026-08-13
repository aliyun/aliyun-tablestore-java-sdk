package com.alicloud.openservices.tablestore.common;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Pure-regex unit test keeping {@link InstanceTableCleanup#DEFAULT_PATTERN} in sync with the
 * table-name families actually produced by the test suites (every
 * {@code OTSHelper.generateUniqueTableName} call site plus the legacy fixed names). No network.
 */
public class InstanceTableCleanupPatternTest {

    private static final Pattern P = Pattern.compile(InstanceTableCleanup.DEFAULT_PATTERN);

    /** Example suffix produced by {@code OTSHelper.generateUniqueTableName}. */
    private static final String UNIQ = "_1784016387343_12";

    /**
     * Every base passed to {@code generateUniqueTableName} across the test sources.
     * If you add a call site with a new base name, add it here AND make sure
     * {@code InstanceTableCleanup.DEFAULT_PATTERN} matches {@code base + "_<ts>_<tid>"}.
     */
    private static final List<String> UNIQUE_NAME_BASES = Arrays.asList(
            "A0", "APIFunctiontest", "BatchWriteTest", "CreateTableTest", "DeleteRowTest",
            "FilterAdvanceFunctionTest", "FilterRestrictionTest", "GetRowTest", "LongNameStream",
            "OTSBasicCompaTest", "ParameterLegalityFunctiontest", "PutRowTest",
            "RestrictedItemFunctiontest", "RestrictedItemLongName", "SDKTestScanTimeseriesData",
            "SDKTestTimeseriesTable", "SDKTestTimeseriesTableOperation", "SecondaryIndexFunctiontest",
            "SystemStatusTestTable", "TableForFailTest", "TableNameFourPK", "TableNameOnePK",
            "TableNameThreePK", "TableNameTwoPK", "TableOptionsFunctiontest", "TestTimeseriesStream",
            "UpdateRowTest", "UpdateTableTest", "_0", "_T", "bool_pk_ft",
            "conditional_update_test_table", "test", "test_analytical_store1",
            "test_analytical_store2", "test_analytical_store3",
            "test_create_and_delete_analytical_store1", "test_create_and_delete_analytical_store2",
            "test_custom_primary_keys", "test_custom_primary_keys_batch_write",
            "test_custom_primary_keys_meta", "test_describe_index_sync_phase",
            "test_no_measurements", "test_update_analytical_store",
            // TimeseriesLastpointIndexTest derives bases from its @Test method names —
            // note testCreateTimeseriesTableWithLastpointIndex does NOT share the
            // testTimeseriesLastpointIndex prefix
            "testCreateTimeseriesTableWithLastpointIndex",
            "testTimeseriesLastpointIndexCreateAndDelete",
            "testTimeseriesLastpointIndexCreateSearchIndexAndSQL",
            "testTimeseriesLastpointIndexStream",
            "testTimeseriesLastpointIndexWithBaseData",
            "testTimeseriesLastpointIndexWithIncrData");

    /** Generic bases that must ONLY match with the unique suffix (bare could be a real table). */
    private static final List<String> GENERIC_BASES = Arrays.asList("test", "_0", "_T", "A0");

    /** Legacy fixed names that historically leaked and must still match bare. */
    private static final List<String> LEGACY_FIXED_NAMES = Arrays.asList(
            "TestTimeseriesStream", "SDKTestTimeseriesTable", "bool_pk_ft", "TableForFailTest",
            "CapacityUnitTest1784016387343", "WideColumnLastpointIndexTest_testLastpointIndexIncrData",
            "TestTableCreationTime", "test_local_txn", "test_custom_primary_keys",
            "test_analytical_store1", "nontxn_sql_tmp1", "RestrictedItemFunctiontest",
            "OTSBasicCompaTest", "FilterAdvanceFunctionTest", "BatchWriteTest", "CreateTableTest",
            "DeleteRowTest", "conditional_update_test_table",
            // fixed table names of the timestream functiontests (classes are not *Test-suffixed,
            // but manual/historical runs can leak these)
            "GetRangIteratorFunctiontest", "PointIteratorFunctiontest");

    /** Names that must NEVER match: plausible real tables on the shared instance.
     *  Includes arbitrary *FunctionTest/*Functiontest-suffixed names: only the enumerated
     *  test families may match, never a suffix wildcard. */
    private static final List<String> PROTECTED_NAMES = Arrays.asList(
            "id", "test", "_0", "_T", "A0", "A1", "table1", "metaTable", "test_1",
            "user_orders", "myFunctionTest", "myFunctionTest2", "myFunctiontest",
            "orderFunctionTest", "testdata", "test_888");

    @Test
    public void uniqueNamesFromEveryCallSiteAreMatched() {
        for (String base : UNIQUE_NAME_BASES) {
            assertTrue("must match unique name: " + base + UNIQ, P.matcher(base + UNIQ).matches());
        }
    }

    @Test
    public void genericBasesOnlyMatchWithUniqueSuffix() {
        for (String base : GENERIC_BASES) {
            assertFalse("bare generic base must NOT match: " + base, P.matcher(base).matches());
            assertTrue("suffixed generic base must match: " + base + UNIQ, P.matcher(base + UNIQ).matches());
        }
    }

    @Test
    public void legacyFixedNamesStillMatch() {
        for (String name : LEGACY_FIXED_NAMES) {
            assertTrue("must match legacy leftover: " + name, P.matcher(name).matches());
        }
    }

    @Test
    public void maxLengthPaddedNamesAreMatched() {
        StringBuilder api = new StringBuilder("LongNameStream" + UNIQ);
        while (api.length() < 255) {
            api.append('a');
        }
        assertTrue(P.matcher(api.toString()).matches());

        StringBuilder restricted = new StringBuilder("RestrictedItemLongName" + UNIQ);
        while (restricted.length() < 255) {
            restricted.append('a');
        }
        assertTrue(P.matcher(restricted.toString()).matches());
    }

    @Test
    public void lastpointIndexDerivedNamesAreMatched() {
        assertTrue(P.matcher("testTimeseriesLastpointIndexStream" + UNIQ + "_lastpoint_index").matches());
    }

    @Test
    public void protectedNamesNeverMatch() {
        for (String name : PROTECTED_NAMES) {
            assertFalse("must NOT match potential real table: " + name, P.matcher(name).matches());
        }
    }
}
