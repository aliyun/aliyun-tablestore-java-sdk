package com.alicloud.openservices.tablestore.timestream.functiontest;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class Helper {
    private static Logger logger = LoggerFactory.getLogger(Helper.class);

    public static void safeClearDB(AsyncClient ots) throws InterruptedException {
        logger.debug("Invoke Helper.safeClearDB");
        try {
            Thread.sleep(1000);
            for (String tableName : Utils.waitForFuture(ots.listTable(null)).getTableNames()) {
                {
                    ListSearchIndexRequest request = new ListSearchIndexRequest();
                    request.setTableName(tableName);
                    ListSearchIndexResponse resp = Utils.waitForFuture(ots.listSearchIndex(request, null));
                    for (SearchIndexInfo info : resp.getIndexInfos()) {
                        DeleteSearchIndexRequest newRequest = new DeleteSearchIndexRequest();
                        newRequest.setTableName(tableName);
                        newRequest.setIndexName(info.getIndexName());
                        Utils.waitForFuture(ots.deleteSearchIndex(newRequest, null));
                    }
                }
                DeleteTableRequest request = new DeleteTableRequest(tableName);
                Utils.waitForFuture(ots.deleteTable(request, null));
            }
        } catch (TableStoreException e) {
            if (!(e.getErrorCode().equals("OTSParameterInvalid") && e.getMessage().endsWith("] not exist."))) {
                throw e;
            }
        }
    }

    public static void waitSync() {
        logger.debug("Invoke Helper.waitSync");
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(80));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static boolean isContaineIdentifier(List<TimestreamIdentifier> metas, TimestreamIdentifier meta) {
        for (TimestreamIdentifier m : metas) {
            if (m.equals(meta)) {
                return true;
            }
        }
        return false;
    }

    public static boolean compareMeta(TimestreamMeta left, TimestreamMeta right) {
        if (left.getIdentifier().equals(right.getIdentifier()) &&
                left.getUpdateTimeInUsec() == right.getUpdateTimeInUsec() &&
                left.getAttributes().equals(right.getAttributes())) {
            return true;
        }
        return false;
    }

    public static boolean isContaineMeta(List<TimestreamMeta> metas, TimestreamMeta meta) {
        for (TimestreamMeta m : metas) {
            if (compareMeta(m, meta)) {
                return true;
            }
        }
        return false;
    }

    public static TimestreamIdentifier getTimestreamMeta(List<TimestreamIdentifier> metas, TimestreamIdentifier meta) {
        for (TimestreamIdentifier m : metas) {
            if (m.equals(meta)) {
                return meta;
            }
        }
        return null;
    }
}
