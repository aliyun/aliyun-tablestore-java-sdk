package examples;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;

public class ComputeSplitsBySizeSample {

    private static final String TABLE_NAME = "testtargettable";
    private static final long SPLIT_SIZE_IN_100MB = 1l;
    private static final String primaryKeyPrefix = "primarykey_";
    private static final String columnNamePrefix = "columnName_";
    private static final int maxNumberPrimaryKeysEachRow = 4;
    private static final int maxNumberColumnsEachRow = 500;
    private static final int maxNumberRowsEachWrite = 200;
    private static final int defaultCuRead = 0;
    private static final int defaultCuWrite = 0;

    public static void main(String[] args) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        SyncClient syncClient = new SyncClient(endPoint, accessId, accessKey,
                instanceName);
        AsyncClient asyncClient = new AsyncClient(endPoint, accessId, accessKey,
                instanceName);

        try {
            // Prepare the target table and test data.
            prepareTableAndData(syncClient);

            // Perform the synchronous ComputeSplitsBySize interface operation
            ComputeSplitsBySizeResponse csbsr = syncComputeSplitsBySize(syncClient);
            List<Split> splits = csbsr.getSplits();
            System.out.println("sync get split: " + splits.size());
            for (Split s: splits) {
                System.out.println(s.jsonize());
            }
            getRange(syncClient,
                    csbsr.getSplits().get(0).getLowerBound(),
                    csbsr.getSplits().get(0).getUpperBound());

            // Perform asynchronous ComputeSplitsBySize interface operation
            Future<ComputeSplitsBySizeResponse> fcsbsrr = asyncComputeSplitsBySize(asyncClient);
            ComputeSplitsBySizeResponse csbsrr = fcsbsrr.get();
            splits = csbsrr.getSplits();
            System.out.println("async get split: " + splits.size());
            for (Split s: splits) {
                System.out.println(s.jsonize());
            }
            getRange(syncClient,
                    csbsrr.getSplits().get(0).getLowerBound(),
                    csbsrr.getSplits().get(0).getUpperBound());
        } catch (TableStoreException e) {
            System.err.println("operation failed, detail: " + e.getMessage());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("request failed, detail: " + e.getMessage());
        } catch (InterruptedException e) {
            System.err.println("request failed, detail: " + e.getMessage());
        } catch (ExecutionException e) {
            System.err.println("request failed, detail: " + e.getMessage());
        } finally {
            syncClient.shutdown();
            asyncClient.shutdown();
        }

    }

    private static GetRangeResponse getRange(SyncClient client, PrimaryKey lowerBound, PrimaryKey upperBound) {
        GetRangeRequest grr = new GetRangeRequest();
        RangeRowQueryCriteria rrqc = new RangeRowQueryCriteria(TABLE_NAME);
        rrqc.setDirection(Direction.FORWARD);
        rrqc.setInclusiveStartPrimaryKey(lowerBound);
        PrimaryKey endPK = upperBound;
        if ( endPK.getPrimaryKeyColumns().length <= 1 ) {
            List<PrimaryKeyColumn> pkcl = new ArrayList<PrimaryKeyColumn>();
            pkcl.add(endPK.getPrimaryKeyColumn(0));
            for ( int i = 1; i < maxNumberPrimaryKeysEachRow; ++i ) {
                String ss = primaryKeyPrefix + i;
                pkcl.add(new PrimaryKeyColumn(ss, PrimaryKeyValue.INF_MIN));
            }
            endPK = new PrimaryKey(pkcl);
        }
        rrqc.setExclusiveEndPrimaryKey(endPK);
        rrqc.setMaxVersions(5);
        grr.setRangeRowQueryCriteria(rrqc);
        return client.getRange(grr);

    }

    private static ComputeSplitsBySizeResponse syncComputeSplitsBySize(SyncClient client) {
        ComputeSplitsBySizeRequest csbsr = new ComputeSplitsBySizeRequest();
        csbsr.setTableName(TABLE_NAME);
        csbsr.setSplitSizeIn100MB(SPLIT_SIZE_IN_100MB);
        return client.computeSplitsBySize(csbsr);
    }

    private static Future<ComputeSplitsBySizeResponse> asyncComputeSplitsBySize(AsyncClient client) {
        ComputeSplitsBySizeRequest csbsr = new ComputeSplitsBySizeRequest();
        csbsr.setTableName(TABLE_NAME);
        csbsr.setSplitSizeInByte(2, 1048576);
        return client.computeSplitsBySize(csbsr, null);
    }

    private static void prepareTableAndData(SyncClient client) {
        // Testing configuration
        String tableName = "testtargettable";
        long maxPutRowTime = 10l * 60l * 1000l;

        // Creating testing data
        boolean ifCreateTable = true;
        ListTableResponse ltr = client.listTable();
        for ( String tn : ltr.getTableNames() ) {
            if ( tn.equals(tableName) ) {
                ifCreateTable = false;
            }
        }
        if ( ifCreateTable ) {
            TableMeta tm = new TableMeta(tableName);
            for (int j = 0; j < maxNumberPrimaryKeysEachRow; ++j) {
                tm.addPrimaryKeyColumn(primaryKeyPrefix + j, PrimaryKeyType.STRING);
            }
            TableOptions to = new TableOptions(86400, 5, 86400);
            ReservedThroughput rtp = new ReservedThroughput( new CapacityUnit(defaultCuRead, defaultCuWrite));
            CreateTableRequest ctr = new CreateTableRequest(tm, to, rtp);
            client.createTable(ctr);
        }
        long startTime = System.currentTimeMillis();
        int bn = 0;
        while( System.currentTimeMillis() - startTime <= maxPutRowTime ) {
            // To insert the testing rows.
            BatchWriteRowRequest bwrr = new BatchWriteRowRequest();
            ++bn;

            for (int i = 0; i < maxNumberRowsEachWrite; ++i) {
                RowPutChange brpc = new RowPutChange(tableName);
                List<PrimaryKeyColumn> pkcl = new ArrayList<PrimaryKeyColumn>();
                for (int j = 0; j < maxNumberPrimaryKeysEachRow; ++j) {
                    String ss = primaryKeyPrefix + j;
                    pkcl.add(new PrimaryKeyColumn(ss, PrimaryKeyValue.fromString(ss + "_" + i + "_" + bn)));
                }
                PrimaryKey bwpk = new PrimaryKey(pkcl);
                brpc.setPrimaryKey(bwpk);
                for (int k = 0; k < maxNumberColumnsEachRow; ++k) {
                    String ss = columnNamePrefix + k;
                    brpc.addColumn(ss, ColumnValue.fromString(ss));
                }
                bwrr.addRowChange(brpc);
            }

            client.batchWriteRow(bwrr);
        }

    }

}
