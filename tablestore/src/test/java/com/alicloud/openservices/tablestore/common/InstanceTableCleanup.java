package com.alicloud.openservices.tablestore.common;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TimeseriesClient;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.DeleteIndexRequest;
import com.alicloud.openservices.tablestore.model.IndexMeta;
import com.alicloud.openservices.tablestore.model.knowledgebase.DeleteKnowledgeBaseRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.ListKnowledgeBaseRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.ListKnowledgeBaseResponse;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesTableRequest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
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
 * <p>{@link #main(String[])} is an explicit, non-interactive entry point for CI wiring that wants to sweep
 * leftovers before a suite; it is NOT hooked into this build, so nothing runs it unless you invoke it
 * yourself. Beware that it deletes without asking: exclusive runs delete all matched leftovers, while shared
 * runs ({@code -Dcleanup.shared=true}) delete only resources older than {@code cleanup.stale.hours} (three
 * hours by default). It skips when service settings are incomplete or {@code -Dcleanup.before.tests.skip=true}
 * is set.
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
    static final String KNOWLEDGE_BASE_PATTERN = "^test_kb_(?:java|doc|retrieve)_.*";
    private static final Pattern KNOWLEDGE_BASE_NAME_PATTERN = Pattern.compile(KNOWLEDGE_BASE_PATTERN);
    private static final Pattern UNIQUE_TIMESTAMP_PATTERN = Pattern.compile("_([0-9]{13})_[0-9]+(?:_|$)");
    private static final long DEFAULT_STALE_HOURS = 3;

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
            + "|test_kb_(?:java|doc|retrieve)_.*"
            + "|JavaSdkKnowledgeBaseQuotaLock"
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
        cleanup(ServiceSettings.load(), Boolean.getBoolean("cleanup.confirm"), isSharedRun());
    }

    /** Invoked once by Maven before the test suite. */
    public static void main(String[] args) throws Exception {
        if (Boolean.getBoolean("cleanup.before.tests.skip")) {
            System.out.println("[cleanup] skipped by cleanup.before.tests.skip");
            return;
        }

        ServiceSettings settings = ServiceSettings.load();
        if (isBlank(settings.getOTSEndpoint()) || isBlank(settings.getOTSAccessKeyId())
                || isBlank(settings.getOTSAccessKeySecret()) || isBlank(settings.getOTSInstanceName())) {
            System.out.println("[cleanup] skipped because TableStore test settings are incomplete");
            return;
        }
        cleanup(settings, true, isSharedRun());
    }

    private static void cleanup(ServiceSettings s, boolean confirm, boolean shared) throws Exception {
        Pattern pattern = Pattern.compile(System.getProperty("cleanup.pattern", DEFAULT_PATTERN));
        long staleHours = Long.getLong("cleanup.stale.hours", DEFAULT_STALE_HOURS);
        long staleBeforeMillis = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(staleHours);
        // -Dcleanup.instance overrides the target instance (e.g. the dedicated instance used by
        // RestrictedItemTest#testCase15); defaults to conf.properties
        String instanceName = System.getProperty("cleanup.instance", s.getOTSInstanceName());
        System.out.println("[cleanup] mode=" + (confirm ? "DELETE" : "DRY-RUN (pass -Dcleanup.confirm=true to delete)")
                + " instance=" + instanceName + " sharing=" + (shared ? "SHARED" : "EXCLUSIVE")
                + (shared ? " staleHours=" + staleHours : "") + " pattern=" + pattern.pattern());

        int deleted = 0, failed = 0;
        SyncClient client = new SyncClient(s.getOTSEndpoint(), s.getOTSAccessKeyId(),
                s.getOTSAccessKeySecret(), instanceName);
        try {
            int[] kbResult = cleanupKnowledgeBases(client, confirm, shared, staleBeforeMillis);
            deleted += kbResult[0];
            failed += kbResult[1];

            List<String> all = client.listTable().getTableNames();
            List<String> matched = new ArrayList<String>();
            List<String> skipped = new ArrayList<String>();
            for (String t : all) {
                boolean delete = pattern.matcher(t).matches()
                        && (!shared || isRegularTableStale(client, t, staleBeforeMillis));
                (delete ? matched : skipped).add(t);
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
                    } catch (TableStoreException e) {
                        if (isNotExist(e)) {
                            continue;
                        }
                        failed++;
                        System.out.println("[cleanup] FAILED table " + t + ": " + e.getMessage());
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
                if (pattern.matcher(t).matches()
                        && (!shared || isTimestampedNameStale(t, staleBeforeMillis))) {
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
                    } catch (TableStoreException e) {
                        if (isNotExist(e)) {
                            continue;
                        }
                        failed++;
                        System.out.println("[cleanup] FAILED timeseries table " + t + ": " + e.getMessage());
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
        if (confirm && failed > 0) {
            throw new AssertionError("Failed to delete " + failed + " leftover test table(s).");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    static boolean isSharedRun() {
        return Boolean.parseBoolean(System.getProperty("cleanup.shared",
                String.valueOf(System.getProperty("test") != null)));
    }

    private static boolean isRegularTableStale(SyncClient client, String tableName, long staleBeforeMillis) {
        try {
            long creationTimeMicros = OTSHelper.describeTable(client, tableName).getCreationTime();
            return creationTimeMicros > 0 && creationTimeMicros / 1000 < staleBeforeMillis;
        } catch (TableStoreException e) {
            return isNotExist(e);
        }
    }

    static boolean isTimestampedNameStale(String tableName, long staleBeforeMillis) {
        Matcher matcher = UNIQUE_TIMESTAMP_PATTERN.matcher(tableName);
        return matcher.find() && Long.parseLong(matcher.group(1)) < staleBeforeMillis;
    }

    private static int[] cleanupKnowledgeBases(SyncClient client, boolean confirm, boolean shared,
                                                long staleBeforeMillis) {
        int deleted = 0;
        int failed = 0;
        String nextToken = null;
        do {
            ListKnowledgeBaseRequest request = new ListKnowledgeBaseRequest();
            request.setMaxResults(100);
            request.setNextToken(nextToken);
            ListKnowledgeBaseResponse response = client.listKnowledgeBase(request);
            if (response.getData() == null) {
                break;
            }
            List<ListKnowledgeBaseResponse.KnowledgeBaseInfo> knowledgeBases =
                    response.getData().getKnowledgeBases();
            if (knowledgeBases != null) {
                for (ListKnowledgeBaseResponse.KnowledgeBaseInfo kb : knowledgeBases) {
                    Long createdAt = kb.getCreatedAt();
                    if (!KNOWLEDGE_BASE_NAME_PATTERN.matcher(kb.getKnowledgeBaseName()).matches()
                            || (shared && (createdAt == null || createdAt >= staleBeforeMillis))) {
                        continue;
                    }
                    System.out.println("[cleanup] knowledge base would-delete: " + kb.getKnowledgeBaseName());
                    if (confirm) {
                        try {
                            DeleteKnowledgeBaseRequest deleteRequest = new DeleteKnowledgeBaseRequest();
                            deleteRequest.setKnowledgeBaseName(kb.getKnowledgeBaseName());
                            client.deleteKnowledgeBase(deleteRequest);
                            OTSHelper.waitForKnowledgeBaseTablesDeleted(client, kb.getKnowledgeBaseName());
                            deleted++;
                            System.out.println("[cleanup] deleted knowledge base: " + kb.getKnowledgeBaseName());
                        } catch (TableStoreException e) {
                            if (!isNotExist(e)) {
                                failed++;
                                System.out.println("[cleanup] FAILED knowledge base "
                                        + kb.getKnowledgeBaseName() + ": " + e.getMessage());
                            }
                        } catch (Exception e) {
                            failed++;
                            System.out.println("[cleanup] FAILED knowledge base "
                                    + kb.getKnowledgeBaseName() + ": " + e.getMessage());
                        }
                    }
                }
            }
            nextToken = response.getData().getNextToken();
        } while (nextToken != null && !nextToken.isEmpty());
        return new int[]{deleted, failed};
    }

    private static boolean isNotExist(TableStoreException e) {
        return ErrorCode.OBJECT_NOT_EXIST.equals(e.getErrorCode())
                || (e.getMessage() != null && e.getMessage().contains("not exist"));
    }
}
