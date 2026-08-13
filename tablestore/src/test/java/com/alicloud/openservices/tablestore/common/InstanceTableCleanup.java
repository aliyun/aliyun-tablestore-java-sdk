package com.alicloud.openservices.tablestore.common;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TimeseriesClient;
import com.alicloud.openservices.tablestore.model.DeleteIndexRequest;
import com.alicloud.openservices.tablestore.model.IndexMeta;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesTableRequest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Targeted, dry-run-first cleanup of leftover test tables on a SHARED TableStore instance.
 *
 * <p>Why: the live-OTS integration suite creates hundreds of tables; most tests clean up in teardown, but a
 * crashed/timed-out run leaves its in-progress tables behind. Across failed runs these accumulate against the
 * instance's 64-table quota and every subsequent run fails with OTSQuotaExhausted.
 *
 * <p>Because the instance is SHARED, this never wipes everything. It deletes only tables whose name matches the
 * tests' distinctive naming families (see {@link #DEFAULT_PATTERN}) — generic names like {@code id}, {@code test},
 * {@code table1} are intentionally NOT matched. And it is DRY-RUN by default: it prints what it would delete and
 * leaves the instance untouched unless {@code -Dcleanup.confirm=true} is passed.
 *
 * <p>Not a gate test: it lives in {@code common} (outside the {@code integration.tests} patterns) and lacks a
 * {@code Test*}/{@code *Test} name, so surefire never auto-selects it. Run it explicitly:
 * <pre>
 *   # 1) preview (safe, deletes nothing):
 *   mvn test -pl tablestore -Dtest=InstanceTableCleanup -DfailIfNoTests=false
 *   # 2) actually delete the matched tables:
 *   mvn test -pl tablestore -Dtest=InstanceTableCleanup -DfailIfNoTests=false -Dcleanup.confirm=true
 *   # optional: override the match (e.g. only SQL test tables):
 *   ... -Dcleanup.pattern='^nontxn_sql_.*'
 * </pre>
 */
public class InstanceTableCleanup {

    /** The {@code _<ts>_<threadId>} suffix appended by {@code OTSHelper.generateUniqueTableName}. */
    static final String UNIQUE_SUFFIX = "_[0-9]+_[0-9]+";

    /**
     * Distinctive integration-test table families only. Three groups:
     * <ol>
     * <li>family names distinctive on their own — matched bare (legacy fixed-name leftovers)
     *     or with the optional {@link #UNIQUE_SUFFIX};</li>
     * <li>generic bases ({@code test}, {@code _0}, {@code _T}, {@code A0}) — matched ONLY with the
     *     mandatory unique suffix, a bare match could hit a real table;</li>
     * <li>max-length-name cases — unique base padded with trailing {@code a}s up to 255 bytes.</li>
     * </ol>
     * Kept in sync with every {@code generateUniqueTableName} call site by
     * {@code InstanceTableCleanupPatternTest}. Deliberately excludes generic names like
     * {@code id}/{@code table1} that could be real tables.
     */
    static final String DEFAULT_PATTERN =
            "^(?:"
            // Functiontest/FunctionTest families are enumerated precisely — a suffix wildcard
            // like ".*FunctionTest" could match a real table (e.g. "myFunctionTest")
            + "(?:APIFunctiontest|ParameterLegalityFunctiontest|RestrictedItemFunctiontest|SecondaryIndexFunctiontest|TableOptionsFunctiontest"
            + "|GetRangIteratorFunctiontest|GetRangeIteratorFunctiontest|PointIteratorFunctiontest|FilterAdvanceFunctionTest"
            + "|nontxn_sql_.*|CapacityUnitTest.*|WideColumnLastpointIndexTest.*|YSTest.*|SDKTest.*|WriterTest.*|TimeseriesWriterTest.*|TestPartition.*"
            + "|TestTableCreationTime|TestTimeseriesStream|JsonUpdateRequestTest|IncrementRequestTest.*"
            + "|OTSBasicCompaTest|SystemStatusTestTable|TableForFailTest"
            + "|TableName(?:OnePK|TwoPK|ThreePK|FourPK)|java_stream_old_new_.*|java_tunnel_old_new_.*"
            + "|test_priority|test_local_txn|test_tunnel_consumer|test_zr|wenxian_searchIndex_spark_test"
            // families introduced/renamed by the parallel-safety refactor (model/* and timeseries suites)
            + "|BatchWriteTest|CreateTableTest|DeleteRowTest|GetRowTest|PutRowTest|UpdateRowTest|UpdateTableTest"
            + "|FilterRestrictionTest|conditional_update_test_table|bool_pk_ft"
            + "|test_analytical_store[0-9]+|test_create_and_delete_analytical_store[0-9]+|test_update_analytical_store"
            + "|test_describe_index_sync_phase|test_custom_primary_keys(?:_meta|_batch_write)?|test_no_measurements"
            // dynamic bases from TimeseriesLastpointIndexTest @Test method names — includes
            // testCreateTimeseriesTableWithLastpointIndex, which does NOT share the
            // testTimeseriesLastpointIndex prefix
            + "|testTimeseriesLastpointIndex[A-Za-z]*|testCreateTimeseriesTableWithLastpointIndex"
            + ")(?:" + UNIQUE_SUFFIX + ")?(?:_lastpoint_index)?"
            // generic bases: only ever match WITH the unique suffix
            + "|(?:test|_0|_T|A0)" + UNIQUE_SUFFIX
            // max-length name cases: unique base padded with trailing 'a's
            + "|(?:RestrictedItemLongName|LongNameStream)" + UNIQUE_SUFFIX + "a*"
            + ")$";

    @Test
    public void cleanup() throws Exception {
        ServiceSettings s = ServiceSettings.load();
        Pattern pattern = Pattern.compile(System.getProperty("cleanup.pattern", DEFAULT_PATTERN));
        boolean confirm = Boolean.getBoolean("cleanup.confirm");
        // -Dcleanup.instance overrides the target instance (e.g. the dedicated instance
        // used by RestrictedItemTest#testCase15); defaults to conf.properties
        String instanceName = System.getProperty("cleanup.instance", s.getOTSInstanceName());
        System.out.println("[cleanup] mode=" + (confirm ? "DELETE" : "DRY-RUN (pass -Dcleanup.confirm=true to delete)")
                + " instance=" + instanceName + " pattern=" + pattern.pattern());

        int deleted = 0, failed = 0;
        SyncClient client = new SyncClient(s.getOTSEndpoint(), s.getOTSAccessKeyId(),
                s.getOTSAccessKeySecret(), instanceName);
        try {
            List<String> all = client.listTable().getTableNames();
            List<String> matched = new ArrayList<String>();
            List<String> skipped = new ArrayList<String>();
            for (String t : all) {
                (pattern.matcher(t).matches() ? matched : skipped).add(t);
            }
            System.out.println("[cleanup] regular tables: " + all.size() + " total, " + matched.size()
                    + " matched, " + skipped.size() + " left alone");
            System.out.println("[cleanup]   would-delete: " + matched);
            System.out.println("[cleanup]   left-alone  : " + skipped);
            if (confirm) {
                for (String t : matched) {
                    try {
                        for (String idx : OTSHelper.listSearchIndex(client, t)) {
                            OTSHelper.deleteSearchIndex(client, t, idx);
                        }
                        for (IndexMeta im : OTSHelper.describeTable(client, t).getIndexMeta()) {
                            client.deleteIndex(new DeleteIndexRequest(t, im.getIndexName()));
                        }
                        OTSHelper.deleteTable(client, t);
                        deleted++;
                        System.out.println("[cleanup] deleted table: " + t);
                    } catch (Exception e) {
                        failed++;
                        System.out.println("[cleanup] FAILED table " + t + ": " + e.getMessage());
                    }
                }
            }
        } finally {
            client.shutdown();
        }

        // timeseries tables live in a separate namespace and need a TimeseriesClient
        TimeseriesClient ts = new TimeseriesClient(s.getOTSEndpoint(), s.getOTSAccessKeyId(),
                s.getOTSAccessKeySecret(), instanceName);
        try {
            List<String> all = ts.listTimeseriesTable().getTimeseriesTableNames();
            List<String> matched = new ArrayList<String>();
            for (String t : all) {
                if (pattern.matcher(t).matches()) {
                    matched.add(t);
                }
            }
            System.out.println("[cleanup] timeseries tables: " + all.size() + " total, " + matched.size()
                    + " matched -> " + matched);
            if (confirm) {
                for (String t : matched) {
                    try {
                        ts.deleteTimeseriesTable(new DeleteTimeseriesTableRequest(t));
                        deleted++;
                        System.out.println("[cleanup] deleted timeseries table: " + t);
                    } catch (Exception e) {
                        failed++;
                        System.out.println("[cleanup] FAILED timeseries table " + t + ": " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("[cleanup] timeseries listing skipped: " + e.getMessage());
        } finally {
            ts.shutdown();
        }

        System.out.println("[cleanup] done. " + (confirm ? ("deleted=" + deleted + " failed=" + failed)
                : "DRY-RUN — nothing deleted."));
    }
}
