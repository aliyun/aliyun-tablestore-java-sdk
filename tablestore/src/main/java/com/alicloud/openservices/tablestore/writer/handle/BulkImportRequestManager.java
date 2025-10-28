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

public class BulkImportRequestManager extends BaseRequestManager {
    private static Logger logger = LoggerFactory.getLogger(BulkImportRequestManager.class);

    public BulkImportRequestManager(AsyncClientInterface ots, WriterConfig writerConfig, BucketConfig bucketConfig, Executor executor,
                                    WriterHandleStatistics writerStatistics, TableStoreCallback<RowChange, RowWriteResult> callback,
                                    Semaphore callbackSemaphore, Semaphore bucketSemaphore) {
        super(ots, writerConfig, bucketConfig, executor, writerStatistics, callback, callbackSemaphore, bucketSemaphore);
    }


    @Override
    public RequestWithGroups makeRequest() {
        if (rowChangeWithGroups.size() > 0) {
            BulkImportRequest request = new BulkImportRequest(bucketConfig.getTableName());
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

    public void sendRequest(RequestWithGroups requestWithGroups) {
        BulkImportRequest finalRequest = (BulkImportRequest) requestWithGroups.getRequest();
        List<Group> finalGroupFuture = requestWithGroups.getGroupList();

        ots.bulkImport(finalRequest, callbackFactory.newInstance(finalGroupFuture));
    }
}
