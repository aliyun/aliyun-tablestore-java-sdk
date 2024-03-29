package com.alicloud.openservices.tablestore.model;

import java.util.Random;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * TableStore SDK支持自定义重试逻辑{@link RetryStrategy}, 重试逻辑用于判断在发生异常时是否需要重试, 并给出本次重试的时间间隔.
 * {@link AlwaysRetryStrategy}为一个重试逻辑示例, 其在最大重试次数内总是重试.
 */
public class AlwaysRetryStrategy implements RetryStrategy {
    private final Random rnd = new Random();
    private int base = 4; // in msec
    private int retries = 0;
    private int maxRetryTimes = 3;
    private int maxRetryPauseInMillis = 1000; // one second

    public AlwaysRetryStrategy() {
    }

    public AlwaysRetryStrategy(int maxRetryTimes, int maxRetryPauseInMillis) {
        Preconditions.checkArgument(maxRetryTimes > 0);
        Preconditions.checkArgument(maxRetryPauseInMillis > 1);

        this.maxRetryTimes = maxRetryTimes;
        this.maxRetryPauseInMillis = maxRetryPauseInMillis;
    }

    public AlwaysRetryStrategy(int initialPauseInMillis, int maxRetryTimes, int maxRetryPauseInMillis) {
        this(maxRetryTimes, maxRetryPauseInMillis);
        Preconditions.checkArgument(initialPauseInMillis > 0);
        this.base = initialPauseInMillis;
    }

    @Override
    public AlwaysRetryStrategy clone() {
        return new AlwaysRetryStrategy(maxRetryTimes, maxRetryPauseInMillis);
    }

    @Override
    public int getRetries() {
        return retries;
    }

    @Override
    public long nextPause(String action, Exception ex) {
        if (retries >= maxRetryTimes) {
            return 0;
        }
        if (base <= 0) {
            return 0;
        }

        int maxPause = 0;
        if (base * 2 < maxRetryPauseInMillis) {
            base *= 2;
            maxPause = base ;
        } else {
            maxPause = maxRetryPauseInMillis;
        }
        int halfPause = maxPause / 2;
        // randomly exponential backoff, in order to make requests sparse.
        long delay = halfPause + rnd.nextInt(maxPause - halfPause);
        ++retries;
        return delay;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public void setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
    }

    public int getMaxRetryPauseInMillis() {
        return maxRetryPauseInMillis;
    }

    public void setMaxRetryPauseInMillis(int maxRetryPauseInMillis) {
        this.maxRetryPauseInMillis = maxRetryPauseInMillis;
    }
}
