package com.alicloud.openservices.tablestore.timeserieswriter.manager;

import com.alicloud.openservices.tablestore.AsyncTimeseriesClientInterface;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataRequest;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesTableRow;
import com.alicloud.openservices.tablestore.timeserieswriter.TimeseriesWriterException;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesRowResult;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesBucketConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesGroup;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesRequestWithGroups;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesRowWithGroup;
import com.alicloud.openservices.tablestore.timeserieswriter.handle.TimeseriesWriterHandleStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

public class TimeseriesBatchRequestManager extends TimeseriesBaseRequestManager{
    private Logger logger = LoggerFactory.getLogger(TimeseriesBatchRequestManager.class);
    public TimeseriesBatchRequestManager(AsyncTimeseriesClientInterface ots, TimeseriesWriterConfig timeseriesWriterConfig, TimeseriesBucketConfig timeseriesBucketConfig, Executor executor, TimeseriesWriterHandleStatistics timeseriesWriterHandleStatistics,
                                         TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback,
                                         Semaphore callbackSemaphore, Semaphore bucketSemaphore) {
        super(ots, timeseriesWriterConfig, timeseriesBucketConfig, executor, timeseriesWriterHandleStatistics,
                callback,
                callbackSemaphore, bucketSemaphore);
    }


    @Override
    public TimeseriesRequestWithGroups makeRequest(String tableName) {
        if (timeseriesRowWithGroups.size() > 0) {
            PutTimeseriesDataRequest request = new PutTimeseriesDataRequest(tableName);
            List<TimeseriesGroup> groupFutureList = new ArrayList<TimeseriesGroup>(timeseriesRowWithGroups.size());

            for (TimeseriesRowWithGroup timeseriesRowWithGroup : timeseriesRowWithGroups) {
                request.addRow(timeseriesRowWithGroup.timeseriesTableRow.getTimeseriesRow());
                groupFutureList.add(timeseriesRowWithGroup.timeseriesGroup);
            }
            clear();
            return new TimeseriesRequestWithGroups(request, groupFutureList);
        }

        return null;
    }

    @Override
    public void sendRequest(TimeseriesRequestWithGroups timeseriesRequestWithGroups) {
        PutTimeseriesDataRequest finalRequest = (PutTimeseriesDataRequest) timeseriesRequestWithGroups.getRequest();
        List<TimeseriesGroup> finalGroupFuture = timeseriesRequestWithGroups.getGroupList();
        TableStoreCallback tableStoreCallback = callbackFactory.newInstance(finalGroupFuture);
        try{
            ots.putTimeseriesData(finalRequest, tableStoreCallback);
        } catch (Exception e) {
            logger.error("Failed while send request:", e);
            tableStoreCallback.onFailed(finalRequest, new TimeseriesWriterException(e.getMessage(), e, "SendRequestError"));
        }
    }
}
