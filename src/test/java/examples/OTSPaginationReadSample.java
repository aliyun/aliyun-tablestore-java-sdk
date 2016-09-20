package examples;

import com.aliyun.openservices.ots.*;
import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.utils.Pair;
import com.aliyun.openservices.ots.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class OTSPaginationReadSample {
    private static final String COLUMN_GID_NAME = "gid";
    private static final String COLUMN_UID_NAME = "uid";
    private static final String COLUMN_NAME_NAME = "name";
    private static final String COLUMN_ADDRESS_NAME = "address";
    private static final String COLUMN_AGE_NAME = "age";
    private static final String COLUMN_MOBILE_NAME = "mobile";

    public static void main(String args[]) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";
        
        OTSClient client = new OTSClient(endPoint, accessId, accessKey,
                instanceName);
        final String tableName = "sampleTable";

        try {
            // 创建表
            createTable(client, tableName);

            // 注意：创建表只是提交请求，OTS创建表需要一段时间。
            // 这里简单地等待5秒，请根据您的实际逻辑修改。
            Thread.sleep(5000);

            // 插入多行数据。
            putRows(client, tableName);

            // 分页查询数据，每一页取5条，直到全部查完.
            readByPage(client, tableName);
        } catch (ServiceException e) {
            System.err.println("操作失败，详情：" + e.getMessage());
            // 可以根据错误代码做出处理， OTS的ErrorCode定义在OTSErrorCode中。
            if (OTSErrorCode.QUOTA_EXHAUSTED.equals(e.getErrorCode())) {
                System.err.println("超出存储配额。");
            }
            // Request ID可以用于有问题时联系客服诊断异常。
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            // 可能是网络不好或者是返回结果有问题
            System.err.println("请求失败，详情：" + e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } finally {
            // 不留垃圾。
            try {
                deleteTable(client, tableName);
            } catch (ServiceException e) {
                System.err.println("删除表格失败，原因：" + e.getMessage());
                e.printStackTrace();
            } catch (ClientException e) {
                System.err.println("删除表格请求失败，原因：" + e.getMessage());
                e.printStackTrace();
            }
            client.shutdown();
        }
    }

    /**
     * 范围查询指定范围内的数据，返回指定页数大小的数据，并能根据offset跳过部分行。
     */
    private static Pair<List<Row>, RowPrimaryKey> readByPage(OTSClient client, String tableName, RowPrimaryKey startKey, RowPrimaryKey endKey, int offset, int pageSize) {
        Preconditions.checkArgument(offset >= 0, "Offset should not be negative.");
        Preconditions.checkArgument(pageSize > 0, "Page size should be greater than 0.");

        List<Row> rows = new ArrayList<Row>(pageSize);
        int limit = pageSize;
        int skip = offset;

        RowPrimaryKey nextStart = startKey;
        while (limit > 0 && nextStart != null) {
            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(nextStart);
            criteria.setExclusiveEndPrimaryKey(endKey);
            criteria.setLimit(skip + limit);

            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            GetRangeResult response = client.getRange(request);
            for (Row row : response.getRows()) {
                if (skip > 0) {
                    skip--;
                } else {
                    rows.add(row);
                    limit--;
                }
            }

            nextStart = response.getNextStartPrimaryKey();
        }

        return new Pair<List<Row>, RowPrimaryKey>(rows, nextStart);
    }

    private static void readByPage(OTSClient client, String tableName) {
        int pageSize = 8;
        int offset = 33;

        RowPrimaryKey startKey = new RowPrimaryKey();
        startKey.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.INF_MIN);
        startKey.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.INF_MIN);

        RowPrimaryKey endKey = new RowPrimaryKey();
        endKey.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.INF_MAX);
        endKey.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.INF_MAX);
        // 读第一页，从范围的offset=33的行开始读起
        Pair<List<Row>, RowPrimaryKey> result = readByPage(client, tableName, startKey, endKey, offset, pageSize);
        for (Row row : result.getFirst()) {
            System.out.println(row.getColumns());
        }
        System.out.println("Total rows count: " + result.getFirst().size());

        // 顺序翻页，读完范围内的所有数据
        startKey = result.getSecond();
        while (startKey != null) {
            System.out.println("============= start read next page ==============");
            result = readByPage(client, tableName, startKey, endKey, 0, pageSize);
            for (Row row : result.getFirst()) {
                System.out.println(row.getColumns());
            }
            startKey = result.getSecond();
            System.out.println("Total rows count: " + result.getFirst().size());
        }
    }

    private static void createTable(OTSClient client, String tableName)
            throws ServiceException, ClientException {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyType.INTEGER);
        // 将该表的读写CU都设置为0
        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        CreateTableRequest request = new CreateTableRequest();
        request.setTableMeta(tableMeta);
        request.setReservedThroughput(capacityUnit);
        client.createTable(request);

        System.out.println("表已创建");
    }

    private static void deleteTable(OTSClient client, String tableName)
            throws ServiceException, ClientException {
        DeleteTableRequest request = new DeleteTableRequest();
        request.setTableName(tableName);
        client.deleteTable(request);

        System.out.println("表已删除");
    }

    private static void putRows(OTSClient client, String tableName)
            throws OTSException, ClientException {
        int bid = 1;
        final int rowCount = 100;
        for (int i = 0; i < rowCount; ++i) {
            RowPutChange rowChange = new RowPutChange(tableName);
            RowPrimaryKey primaryKey = new RowPrimaryKey();
            primaryKey.addPrimaryKeyColumn(COLUMN_GID_NAME,
                    PrimaryKeyValue.fromLong(bid));
            primaryKey.addPrimaryKeyColumn(COLUMN_UID_NAME,
                    PrimaryKeyValue.fromLong(i));
            rowChange.setPrimaryKey(primaryKey);
            rowChange.addAttributeColumn(COLUMN_NAME_NAME,
                    ColumnValue.fromString("小" + Integer.toString(i)));
            rowChange.addAttributeColumn(COLUMN_MOBILE_NAME,
                    ColumnValue.fromString("111111111"));
            rowChange.addAttributeColumn(COLUMN_ADDRESS_NAME,
                    ColumnValue.fromString("中国A地"));
            rowChange.addAttributeColumn(COLUMN_AGE_NAME,
                    ColumnValue.fromLong(20));
            rowChange.setCondition(new Condition(
                    RowExistenceExpectation.EXPECT_NOT_EXIST));

            PutRowRequest request = new PutRowRequest();
            request.setRowChange(rowChange);

            PutRowResult result = client.putRow(request);
            int consumedWriteCU = result.getConsumedCapacity()
                    .getCapacityUnit().getWriteCapacityUnit();

            System.out.println("成功插入数据, 消耗的写CU为：" + consumedWriteCU);
        }

        System.out.println(String.format("成功插入%d行数据。", rowCount));
    }
}
