package com.alicloud.openservices.tablestore.writer.handle;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.*;
import com.alicloud.openservices.tablestore.writer.config.BucketConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

public class BatchWriteRowRequestManager extends BaseRequestManager {
    private static Logger logger = LoggerFactory.getLogger(BatchWriteRowRequestManager.class);

    public BatchWriteRowRequestManager(AsyncClientInterface ots, WriterConfig writerConfig, BucketConfig bucketConfig, Executor executor,
                                       WriterHandleStatistics writerStatistics, TableStoreCallback<RowChange, RowWriteResult> callback,
                                       Semaphore callbackSemaphore, Semaphore bucketSemaphore) {
        super(ots, writerConfig, bucketConfig, executor, writerStatistics, callback, callbackSemaphore, bucketSemaphore);
    }

    @Override
    public RequestWithGroups makeRequest() {
        if (rowChangeWithGroups.size() > 0) {
            BatchWriteRowRequest request = new BatchWriteRowRequest();
            List<Group> groupFutureList = new ArrayList<Group>(rowChangeWithGroups.size());

            for (RowChangeWithGroup rowChangeWithGroup : rowChangeWithGroups) {
                request.addRowChange(rowChangeWithGroup.rowChange);
                groupFutureList.add(rowChangeWithGroup.group);
            }
            clear();
            return new RequestWithGroups(request, groupFutureList);
        }

        return null;
    }

    @Override
    public void sendRequest(RequestWithGroups requestWithGroups) {
        BatchWriteRowRequest finalRequest = (BatchWriteRowRequest) requestWithGroups.getRequest();
        List<Group> finalGroupFuture = requestWithGroups.getGroupList();

        ots.batchWriteRow(finalRequest, callbackFactory.newInstance(finalGroupFuture));
    }
}
