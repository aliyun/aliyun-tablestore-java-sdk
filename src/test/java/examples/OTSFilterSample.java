package examples;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.OTSErrorCode;
import com.aliyun.openservices.ots.ServiceException;
import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.model.condition.ColumnCondition;
import com.aliyun.openservices.ots.model.condition.CompositeCondition;
import com.aliyun.openservices.ots.model.condition.RelationalCondition;

import java.util.Iterator;
import java.util.Random;

/**
 * 该示例代码包含了如何使用OTS的filter功能
 */
public class OTSFilterSample {

    private static final String COLUMN_GID_NAME = "gid";
    private static final String COLUMN_UID_NAME = "uid";
    private static final String COLUMN_NAME_NAME = "name";
    private static final String COLUMN_ADDRESS_NAME = "address";
    private static final String COLUMN_AGE_NAME = "age";
    private static final String COLUMN_IS_STUDENT_NAME = "isstudent";

    public static void main(String args[]) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";
        
        OTSClient client = new OTSClient(endPoint, accessId, accessKey, instanceName); 
        final String tableName = "FilterSampleTable";

        try{
            // 创建表
            createTable(client, tableName);

            // 注意：创建表只是提交请求，OTS创建表需要一段时间。
            // 这里简单地等待5秒，请根据您的实际逻辑修改。
            Thread.sleep(5 * 1000);

            // 构造一些数据供查询测试
            generateData(client, tableName);

            getRowWithFilter(client, tableName);

            getRangeWithFilter(client, tableName);
        } catch(ServiceException e) {
            System.err.println("操作失败，详情：" + e.getMessage());
            // 可以根据错误代码做出处理， OTS的ErrorCode定义在OTSErrorCode中。
            if (OTSErrorCode.QUOTA_EXHAUSTED.equals(e.getErrorCode())){
                System.err.println("超出存储配额。");
            }
            // Request ID可以用于有问题时联系客服诊断异常。
            System.err.println("Request ID:" + e.getRequestId());
        } catch(ClientException e) {
            // 可能是网络不好或者是返回结果有问题
            System.err.println("请求失败，详情：" + e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } finally{
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

    private static void createTable(OTSClient client, String tableName)
            throws ServiceException, ClientException{
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

    private static void generateData(OTSClient client, String tableName)
            throws ServiceException, ClientException{
        for (int i = 0; i < 100; i++) {
            RowPutChange rowChange = new RowPutChange(tableName);
            RowPrimaryKey primaryKey = new RowPrimaryKey();
            primaryKey.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.fromLong(i));
            primaryKey.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.fromLong(i + 1));
            rowChange.setPrimaryKey(primaryKey);
            rowChange.addAttributeColumn(COLUMN_NAME_NAME, ColumnValue.fromString("lilei"));
            rowChange.addAttributeColumn(COLUMN_ADDRESS_NAME, ColumnValue.fromString("somewhere"));
            rowChange.addAttributeColumn(COLUMN_AGE_NAME, ColumnValue.fromLong(new Random().nextInt(30) + 10));
            rowChange.addAttributeColumn(COLUMN_IS_STUDENT_NAME, ColumnValue.fromBoolean(i % 2 == 0));
            rowChange.setCondition(new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST));

            PutRowRequest request = new PutRowRequest();
            request.setRowChange(rowChange);

            client.putRow(request);
        }
    }

    private static void getRowWithFilter(OTSClient client, String tableName)
            throws ServiceException, ClientException{

        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        RowPrimaryKey primaryKeys = new RowPrimaryKey();
        primaryKeys.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.fromLong(1));
        primaryKeys.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.fromLong(2));
        criteria.setPrimaryKey(primaryKeys);

        // 增加一个查询条件，只有当满足条件 name == 'lilei' 时才返回数据。
        RelationalCondition filter = new RelationalCondition(COLUMN_NAME_NAME, RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("lilei"));
        criteria.setFilter(filter);

        GetRowRequest request = new GetRowRequest();
        request.setRowQueryCriteria(criteria);
        GetRowResult result = client.getRow(request);
        Row row = result.getRow();
        System.out.println("Row returned (name == 'lilei'): " + row.getColumns());

        // 更改查询条件，只有当满足条件(name == 'lilei' and isstudent == true)时才返回数据。
        RelationalCondition anotherFilter = new RelationalCondition(COLUMN_IS_STUDENT_NAME, RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromBoolean(true));
        CompositeCondition cc = new CompositeCondition(CompositeCondition.LogicOperator.AND);
        cc.addCondition(filter);
        cc.addCondition(anotherFilter);

        criteria.setFilter(cc);
        // 而目前表中的该行数据不满足该条件，所以不会返回。
        result = client.getRow(request);
        row = result.getRow();
        System.out.println("Row returned (name == 'lilei' and isstudent == true): " + row.getColumns());
    }

    private static void getRangeWithFilter(OTSClient client, String tableName)
        throws ServiceException, ClientException {
        // 查询范围为：
        //      start primary key = (gid=0, INF_MIN)
        //      end primary key   = (gid=100, INF_MAX)
        // 且满足条件为：
        //      isstudent == true
        // 的所有行。
        RangeIteratorParameter param = new RangeIteratorParameter(tableName);
        RowPrimaryKey startPk = new RowPrimaryKey();
        startPk.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.fromLong(0));
        startPk.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.INF_MIN);
        RowPrimaryKey endPk = new RowPrimaryKey();
        endPk.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.fromLong(100));
        endPk.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.INF_MAX);

        param.setInclusiveStartPrimaryKey(startPk);
        param.setExclusiveEndPrimaryKey(endPk);

        RelationalCondition filter = new RelationalCondition(COLUMN_IS_STUDENT_NAME, RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromBoolean(true));
        param.setFilter(filter);

        Iterator<Row> rowIter = client.createRangeIterator(param);
        int totalRows = 0;
        while (rowIter.hasNext()) {
            Row row = rowIter.next();
            totalRows++;
            System.out.println(row);
        }

        System.out.println("Total rows read: " + totalRows);
    }
    
    private static void deleteTable(OTSClient client, String tableName)
            throws ServiceException, ClientException{
        DeleteTableRequest request = new DeleteTableRequest();
        request.setTableName(tableName);
        client.deleteTable(request);

        System.out.println("表已删除");
    }
}
