/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package examples;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.OTSErrorCode;
import com.aliyun.openservices.ots.ServiceException;
import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.model.condition.ColumnCondition;
import com.aliyun.openservices.ots.model.condition.CompositeCondition;
import com.aliyun.openservices.ots.model.condition.RelationalCondition;
import com.aliyun.openservices.ots.protocol.OtsProtocol2;

/**
 * 该示例代码包含了如何使用OTS的Conditional update功能
 */
public class OTSConditionalUpdateSample {

    private static final String COLUMN_GID_NAME = "gid";
    private static final String COLUMN_UID_NAME = "uid";
    private static final String COLUMN_NAME_NAME = "name";
    private static final String COLUMN_ADDRESS_NAME = "address";
    private static final String COLUMN_AGE_NAME = "age";
    private static final String COLUMN_MOBILE_NAME = "mobile";

    public static void main(String args[]) {
        final String endPoint = "http://10.101.168.94";
        final String accessId = "xxxx";
        final String accessKey = "yyyy";
        final String instanceName = "zzzz";
        
        OTSClient client = new OTSClient(endPoint, accessId, accessKey, instanceName); 
        final String tableName = "ConditionalUpdateSampleTable";

        try{
            // 创建表
            createTable(client, tableName);

            // 注意：创建表只是提交请求，OTS创建表需要一段时间。
            // 这里简单地等待10秒，请根据您的实际逻辑修改。
            Thread.sleep(10 * 1000);

            // 插入一条数据。
            putRow(client, tableName);
            // 再取回来看看。
            getRow(client, tableName);
            // 改一下这条数据。

            // 设置update condition为：年龄小于20岁
            ColumnCondition cond = new RelationalCondition(
                    COLUMN_AGE_NAME, RelationalCondition.CompareOperator.LESS_THAN,
                    ColumnValue.fromLong(20));
            // 这时update应该失败
            updateRow(client, tableName, cond);
            getRow(client, tableName);

            // 设置update condition为：年龄大于等于20 并且 地址是中国A地
            cond = new CompositeCondition(CompositeCondition.LogicOperator.AND)
            .addCondition(new RelationalCondition(
                    COLUMN_AGE_NAME, RelationalCondition.CompareOperator.GREATER_EQUAL,
                    ColumnValue.fromLong(20)))
            .addCondition(new RelationalCondition(
                    COLUMN_ADDRESS_NAME, RelationalCondition.CompareOperator.EQUAL,
                    ColumnValue.fromString("中国A地")));
            // 这时update应该成功
            updateRow(client, tableName, cond);
            getRow(client, tableName);

            // 删除这条数据。
            deleteRow(client, tableName);

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
        // 将该表的读写CU都设置为100
        CapacityUnit capacityUnit = new CapacityUnit(100, 100);

        CreateTableRequest request = new CreateTableRequest();
        request.setTableMeta(tableMeta);
        request.setReservedThroughput(capacityUnit);
        client.createTable(request);

        System.out.println("表已创建");
    }

    private static void putRow(OTSClient client, String tableName)
            throws ServiceException, ClientException{
        RowPutChange rowChange = new RowPutChange(tableName);
        RowPrimaryKey primaryKey = new RowPrimaryKey();
        primaryKey.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.fromLong(1));
        primaryKey.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.fromLong(101));
        rowChange.setPrimaryKey(primaryKey);
        rowChange.addAttributeColumn(COLUMN_NAME_NAME, ColumnValue.fromString("张三"));
        rowChange.addAttributeColumn(COLUMN_MOBILE_NAME, ColumnValue.fromString("111111111"));
        rowChange.addAttributeColumn(COLUMN_ADDRESS_NAME, ColumnValue.fromString("中国A地"));
        rowChange.addAttributeColumn(COLUMN_AGE_NAME, ColumnValue.fromLong(20));
        rowChange.setCondition(new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST)); 
        
        PutRowRequest request = new PutRowRequest();
        request.setRowChange(rowChange);

        PutRowResult result = client.putRow(request);
        int consumedWriteCU = result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit();

        System.out.println("成功插入数据, 消耗的写CapacityUnit为：" + consumedWriteCU);
    }

    private static void getRow(OTSClient client, String tableName)
            throws ServiceException, ClientException{

        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        RowPrimaryKey primaryKeys = new RowPrimaryKey();
        primaryKeys.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.fromLong(1));
        primaryKeys.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.fromLong(101));
        criteria.setPrimaryKey(primaryKeys);
        criteria.addColumnsToGet(new String[] {
                COLUMN_NAME_NAME,
                COLUMN_ADDRESS_NAME,
                COLUMN_AGE_NAME
        });

        GetRowRequest request = new GetRowRequest();
        request.setRowQueryCriteria(criteria);
        GetRowResult result = client.getRow(request);
        Row row = result.getRow();
        
        int consumedReadCU = result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit();
        System.out.println("本次读操作消耗的读CapacityUnti为：" + consumedReadCU);
        System.out.println("name信息：" + row.getColumns().get(COLUMN_NAME_NAME));
        System.out.println("address信息：" + row.getColumns().get(COLUMN_ADDRESS_NAME));
        System.out.println("age信息：" + row.getColumns().get(COLUMN_AGE_NAME));
    }
    
    private static boolean updateRow(OTSClient client, String tableName, ColumnCondition cond) {
        try {
            RowUpdateChange rowChange = new RowUpdateChange(tableName);
            RowPrimaryKey primaryKeys = new RowPrimaryKey();
            primaryKeys.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.fromLong(1));
            primaryKeys.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.fromLong(101));
            rowChange.setPrimaryKey(primaryKeys);
            // 更新以下三列的值
            rowChange.addAttributeColumn(COLUMN_NAME_NAME, ColumnValue.fromString("张三"));
            rowChange.addAttributeColumn(COLUMN_ADDRESS_NAME, ColumnValue.fromString("中国B地"));
            // 删除mobile和age信息
            rowChange.deleteAttributeColumn(COLUMN_MOBILE_NAME);
            rowChange.deleteAttributeColumn(COLUMN_AGE_NAME);

            // 设置update condition为"年龄小于25"
            Condition condition = new Condition(RowExistenceExpectation.IGNORE);
            condition.setColumnCondition(cond);
            rowChange.setCondition(condition);

            UpdateRowRequest request = new UpdateRowRequest();
            request.setRowChange(rowChange);

            UpdateRowResult result = client.updateRow(request);
            int consumedWriteCU = result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit();
            System.out.println("成功更新数据, 消耗的写CapacityUnit为：" + consumedWriteCU);

            return true;
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
        }

        return false;
    }
    
    private static void deleteRow(OTSClient client, String tableName)
            throws ServiceException, ClientException{
        RowDeleteChange rowChange = new RowDeleteChange(tableName);
        RowPrimaryKey primaryKeys = new RowPrimaryKey();
        primaryKeys.addPrimaryKeyColumn(COLUMN_GID_NAME, PrimaryKeyValue.fromLong(1));
        primaryKeys.addPrimaryKeyColumn(COLUMN_UID_NAME, PrimaryKeyValue.fromLong(101));
        rowChange.setPrimaryKey(primaryKeys);
        
        DeleteRowRequest request = new DeleteRowRequest();
        request.setRowChange(rowChange);

        DeleteRowResult result = client.deleteRow(request);
        int consumedWriteCU = result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit();

        System.out.println("成功删除数据, 消耗的写CapacityUnit为：" + consumedWriteCU);
    }

    private static void deleteTable(OTSClient client, String tableName)
            throws ServiceException, ClientException{
        DeleteTableRequest request = new DeleteTableRequest();
        request.setTableName(tableName);
        client.deleteTable(request);

        System.out.println("表已删除");
    }
}
