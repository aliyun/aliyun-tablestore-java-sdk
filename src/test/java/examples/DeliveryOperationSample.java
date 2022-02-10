package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.delivery.*;

public class DeliveryOperationSample {

    public static void main(String[] args) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        SyncClient client = new SyncClient(endPoint, accessId, accessKey, instanceName);
        try {
            createDeliveryTask(client);
//            deleteDeliveryTask(client);
            describeDeliveryTask(client);
            listDeliveryTask(client);
            System.out.println("end");
        } catch (TableStoreException e) {
            System.err.println("操作失败，详情：" + e.getMessage() + e.getErrorCode() + e.toString());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("请求失败，详情：" + e.getMessage());
        } finally {
            client.shutdown();
        }
    }

    private static void createDeliveryTask(SyncClient client) {
        String tableName = "TaskInfo";
        String taskName = "taskTest_29";
        OSSTaskConfig taskConfig = new OSSTaskConfig();
        taskConfig.setOssPrefix("taskTest_02/year=$yyyy/month=$MM");
        taskConfig.setOssBucket("ots-delivery-hz");
        taskConfig.setOssEndpoint("oss-cn-hangzhou.aliyuncs.com");
        taskConfig.setOssStsRole("aliyunotsdeliveryrole");
        //optional
        EventColumn eventColumn = new EventColumn("TaskInfo", EventTimeFormat.RFC1123);
        taskConfig.setEventTimeColumn(eventColumn);
        taskConfig.addParquetSchema(new ParquetSchema("TaskID", "TaskID", DataType.UTF8));
        taskConfig.addParquetSchema(new ParquetSchema("SoftDelete", "SoftDelete", DataType.BOOL));
        taskConfig.addParquetSchema(new ParquetSchema("Config", "Config", DataType.UTF8));
        CreateDeliveryTaskRequest request = new CreateDeliveryTaskRequest();
        request.setTableName(tableName);
        request.setTaskName(taskName);
        request.setTaskConfig(taskConfig);
        request.setTaskType(DeliveryTaskType.BASE_INC);
        CreateDeliveryTaskResponse response = client.createDeliveryTask(request);
        System.out.println("resquestID: "+ response.getRequestId());
        System.out.println("traceID: " + response.getTraceId());
        System.out.println("create delivery task success");
    }

    private static void deleteDeliveryTask(SyncClient client) {
        String tableName = "TaskInfo";
        String taskName = "testTask_29";
        DeleteDeliveryTaskRequest request = new DeleteDeliveryTaskRequest(tableName, taskName);
        DeleteDeliveryTaskResponse response = client.deleteDeliveryTask(request);
        System.out.println("resquestID: "+ response.getRequestId());
        System.out.println("traceID: " + response.getTraceId());
        System.out.println("delete task delivery success");
    }

    public static void describeDeliveryTask(SyncClient client) {
        String tableName = "TaskInfo";
        String taskName  = "taskTest_25";
        DescribeDeliveryTaskRequest request = new DescribeDeliveryTaskRequest(tableName, taskName);
        DescribeDeliveryTaskResponse response = client.describeDeliveryTask(request);
        System.out.println("resquestID: "+ response.getRequestId());
        System.out.println("traceID: " + response.getTraceId());
        System.out.println("OSSconfig: " + response.getTaskConfig());
        System.out.println("TaskSyncStat: " + response.getTaskSyncStat());
        System.out.println("taskType: " + response.getTaskType());
    }

    public static void listDeliveryTask(SyncClient client) {
        String tableName = "TaskInfo";
        ListDeliveryTaskRequest request = new ListDeliveryTaskRequest(tableName);
        ListDeliveryTaskResponse response = client.listDeliveryTask(request);
        System.out.println("resquestID: "+ response.getRequestId());
        System.out.println("traceID: " + response.getTraceId());
        for(DeliveryTaskInfo taskInfo: response.getTaskInfos()) {
            System.out.println("tableName: " + taskInfo.getTableName());
            System.out.println("taskName: " + taskInfo.getTaskName());
            System.out.println("taskType: " + taskInfo.getTaskType().toString());
        }
    }
}

