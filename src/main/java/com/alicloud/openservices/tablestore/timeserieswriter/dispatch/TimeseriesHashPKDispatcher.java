package com.alicloud.openservices.tablestore.timeserieswriter.dispatch;

import com.alicloud.openservices.tablestore.DefaultTableStoreTimeseriesWriter;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesKey;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicLong;

public class TimeseriesHashPKDispatcher extends TimeseriesBaseDispatcher {
    private Logger logger = LoggerFactory.getLogger(TimeseriesHashPKDispatcher.class);

    private AtomicLong counter = new AtomicLong(0);
    private int bucketCount;

    public TimeseriesHashPKDispatcher(int bucketCount) {
        super(bucketCount);
        this.bucketCount = Math.min(bucketCount, 256);
    }

    @Override
    public int getDispatchIndex(TimeseriesRow timeseriesRow) {
        if (bucketCount == 1) {
            addBucketCount(0);
            return 0;
        }

        TimeseriesKey key = timeseriesRow.getTimeseriesKey();
        try {
            MessageDigest md5 = MessageDigest.getInstance("md5");
            md5.update(key.getMeasurementName().getBytes());
            md5.update(key.getDataSource().getBytes());
            md5.update(key.buildTagsString().getBytes());
            byte[] digest = md5.digest();
            int bucketIndex = digest[0];
            int capacity = 256 / bucketCount;
            bucketIndex = ((bucketIndex + 128) / capacity + bucketCount) % bucketCount;
            addBucketCount(bucketIndex);
            return bucketIndex;
        } catch (NoSuchAlgorithmException e) {
            logger.error("Failed while get hash dispatch index,:", e);
            return 0;
        } catch (IllegalArgumentException e){
            logger.error("Failed while get hash dispatch index,:", e);
            return 0;
        }


    }

}