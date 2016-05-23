package com.aliyun.openservices.ots.internal;

public class OTSAlwaysRetryStrategy implements OTSRetryStrategy {

    private int retryPauseInMillis = 50; // milliseconds
    private int maxRetryTimes = 3;
    private int maxRetryPauseInMillis = 1000; // one second

    public int getRetryPauseInMillis() {
        return retryPauseInMillis;
    }

    public void setRetryPauseInMillis(int retryPauseInMillis) {
        this.retryPauseInMillis = retryPauseInMillis;
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

    /**
     * 若重试次数未超过上限，则重试。
     *
     * @param action  操作名，比如"ListTable"、"GetRow"、"PutRow"等
     * @param ex      上次访问失败的错误信息、为ClientException或OTSException
     * @param retries 表示本次判断的为第retries次重试，retries 大于 0
     * @return
     */
    @Override
    public boolean shouldRetry(String action, Exception ex, int retries) {
        if (retries > maxRetryTimes) {
            return false;
        }

        return true;
    }

    /**
     * 获取重试的时间间隔，重试时间间隔的计算公式为：Math.min(Match.pow(2, reties) * {@link #retryPauseInMillis}, {@link #maxRetryPauseInMillis})
     *
     * @param action  操作名，比如"ListTable"、"GetRow"、"PutRow"等
     * @param ex      上次访问失败的错误信息、为ClientException或OTSException
     * @param retries 表示将要发起第retries次重试， retries 大于 0
     * @return
     */
    @Override
    public long getPauseDelay(String action, Exception ex, int retries) {
        // make the pause time increase exponentially
        // based on an assumption that the more times it retries,
        // the less probability it succeeds.
        int scale = retryPauseInMillis;
        long delay = (long) Math.pow(2, retries) * scale;
        return Math.min(delay, maxRetryPauseInMillis);
    }

}
