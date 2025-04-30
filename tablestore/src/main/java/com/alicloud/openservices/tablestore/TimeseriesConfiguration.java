package com.alicloud.openservices.tablestore;

public class TimeseriesConfiguration {

    private long metaCacheMaxDataSize = 64 * 1024 * 1024;
    private int metaCacheExpireTimeAfterAccessInSec = 3600;

    public TimeseriesConfiguration() {}

    public long getMetaCacheMaxDataSize() {
        return metaCacheMaxDataSize;
    }

    public void setMetaCacheMaxDataSize(long metaCacheMaxDataSize) {
        this.metaCacheMaxDataSize = metaCacheMaxDataSize;
    }

    public int getMetaCacheExpireTimeAfterAccessInSec() {
        return metaCacheExpireTimeAfterAccessInSec;
    }

    public void setMetaCacheExpireTimeAfterAccessInSec(int metaCacheExpireTimeAfterAccessInSec) {
        this.metaCacheExpireTimeAfterAccessInSec = metaCacheExpireTimeAfterAccessInSec;
    }
}
