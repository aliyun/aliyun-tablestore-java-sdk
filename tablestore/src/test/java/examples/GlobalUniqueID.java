package examples;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;

/**
 * GlobalUniqueID: [bucket id] + [auto increment id]
 * Table Schema: BUCKET_ID[INTEGER], ID[AUTO_INCREMENT]
 *
 * This class is thread safe.
 */
public class GlobalUniqueID {
    private SyncClient client;
    private String tableName;
    final private long BUCKET_COUNT = 4096; // max 12 bits
    final private long MAX_SUB_ID = 1L << 52; // 52 bits
    final private String PK_0 = "b";
    final private String PK_1 = "i";
    final private String COL_0 = "_t";

    public GlobalUniqueID(SyncClient client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    public boolean isTableExist() {
        DescribeTableRequest request = new DescribeTableRequest();
        request.setTableName(tableName);

        DescribeTableResponse response;
        try {
            response = client.describeTable(request);
        } catch(TableStoreException e) {
            if (e.getErrorCode().equals("OTSObjectNotExist")) {
                return false;
            }
            throw e;
        }

        // check meta
        TableMeta tableMeta = response.getTableMeta();
        boolean metaCheckOK = true;
        if (tableMeta.getPrimaryKeyList().size() == 2) {
            PrimaryKeySchema pk0 = tableMeta.getPrimaryKeyList().get(0);
            if (!pk0.getName().equals(PK_0) || pk0.getType() != PrimaryKeyType.INTEGER) {
                metaCheckOK = false;
            }

            PrimaryKeySchema pk1 = tableMeta.getPrimaryKeyList().get(1);
            if (!pk1.getName().equals(PK_1) || pk1.getType() != PrimaryKeyType.INTEGER || pk1.getOption() != PrimaryKeyOption.AUTO_INCREMENT) {
                metaCheckOK = false;
            }
        } else {
            metaCheckOK = false;
        }

        if (!metaCheckOK) {
            throw new IllegalStateException("Table schema is invalid.");
        }

        return true;
    }

    public long generateUniqueID(Object obj) {
        return generateUniqueID(obj.hashCode());
    }

    public long generateUniqueID(int hashCode) {
        long bucketID = Math.abs(hashCode) % BUCKET_COUNT;

        RowPutChange rpc = new RowPutChange(tableName);
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn(PK_0, PrimaryKeyValue.fromLong(bucketID))
                .addPrimaryKeyColumn(PK_1, PrimaryKeyValue.AUTO_INCREMENT).build();
        rpc.setPrimaryKey(primaryKey);
        rpc.addColumn(COL_0, ColumnValue.fromLong(System.currentTimeMillis()));
        rpc.setReturnType(ReturnType.RT_PK);

        PutRowResponse response = client.putRow(new PutRowRequest(rpc));
        long subId = response.getRow().getPrimaryKey().getPrimaryKeyColumn(1).getValue().asLong();
        if (subId >= MAX_SUB_ID) {
            throw new IllegalStateException("Auto increment id reach maximum: " + subId);
        }

        System.out.println(bucketID);
        System.out.println(subId);
        long uniqueID = (bucketID << 52) | (subId & 0x000fffffffffffffL);
        return uniqueID;
    }

    public void createTable() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(PK_0, PrimaryKeyType.INTEGER);
        tableMeta.addAutoIncrementPrimaryKeyColumn(PK_1);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setTimeToLive(-1);
        tableOptions.setMaxVersions(1);
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        client.createTable(request);
    }

    public static void main(String[] args) {
        SyncClient client = new SyncClient("", "", "", "");
        GlobalUniqueID gui = new GlobalUniqueID(client, "GUI");
        long uniqueID = gui.generateUniqueID(1029182172121L);
        System.out.println(uniqueID);
        client.shutdown();
    }
}
