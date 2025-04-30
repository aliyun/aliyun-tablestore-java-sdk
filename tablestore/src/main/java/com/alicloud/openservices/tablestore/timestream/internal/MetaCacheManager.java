package com.alicloud.openservices.tablestore.timestream.internal;

import com.alicloud.openservices.tablestore.TableStoreWriter;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

import com.google.common.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaCacheManager {
    private Logger logger = LoggerFactory.getLogger(MetaCacheManager.class);

    /**
     * Key is TimestreamMeta
     * Value is the corresponding last update time, in units of us
     */
    private String tableName;
    private Cache<TimestreamIdentifier, Long> cache;
    private long intervalDumpMeta;
    private TableStoreWriter writer;

    public MetaCacheManager(String tableName, long intervalDumpMeta, long maxCacheSize, TableStoreWriter writer) {
        this.tableName = tableName;
        this.intervalDumpMeta = intervalDumpMeta;
        this.cache = CacheBuilder.newBuilder()
                .expireAfterWrite(intervalDumpMeta, TimeUnit.SECONDS)
                .weigher(
                        new Weigher<TimestreamIdentifier, Long>() {
                            public int weigh(TimestreamIdentifier k, Long v) {
                                return k.getDataSize() + 8;
                            }
                        }
                )
                .maximumWeight(maxCacheSize)
                .build();
        this.writer = writer;
    }

    public void close() {
        this.writer.flush();
    }

    public void addTimestreamMeta(TimestreamIdentifier identifier, long updateTime) {
        Long lastUpdateTime = this.cache.getIfPresent(identifier);
        if (lastUpdateTime == null || (lastUpdateTime + TimeUnit.SECONDS.toMicros(intervalDumpMeta)) <= updateTime) {
            // The time of the last update on the timeline has exceeded the set time from the current time, so re-update the timeline.
            RowChange rowChange = Utils.serializeTimestreamIdentifier(tableName, identifier, updateTime);
            if (!this.writer.tryAddRowChange(rowChange)) {
                logger.error("Failed update meta: " + identifier.toString());
            } else {
                this.cache.put(identifier, updateTime);
            }
        }
    }

    public void updateTimestreamMeta(TimestreamIdentifier meta, long updateTime) {
        Long lastUpdateTime = this.cache.getIfPresent(meta);
        if (lastUpdateTime == null || lastUpdateTime <= updateTime) {
            // The time of the last update on the timeline has exceeded the set time from the current moment, re-update the timeline.
            this.cache.put(meta, updateTime);
        }
    }

    protected Long getTimestreamMetaLastUpdateTime(TimestreamIdentifier meta) {
        return this.cache.getIfPresent(meta);
    }
}
