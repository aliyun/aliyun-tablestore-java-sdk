package com.aliyun.openservices.ots.internal;

public interface OTSRetryStrategy {

    /**
     * 返回是否需要发起第retries次重试。
     *
     * @param action  操作名，比如"ListTable"、"GetRow"、"PutRow"等
     * @param ex      上次访问失败的错误信息、为ClientException或OTSException
     * @param retries 表示本次判断的为第retries次重试，retries 大于 0
     * @return 是否需要进行第retries次重试
     */
    public boolean shouldRetry(String action, Exception ex, int retries);

    /**
     * 得到发起第retries次重试前延迟的时间。SDK会在这一段时间之后发起第retries次重试。
     *
     * @param action  操作名，比如"ListTable"、"GetRow"、"PutRow"等
     * @param ex      上次访问失败的错误信息、为ClientException或OTSException
     * @param retries 表示将要发起第retries次重试， retries 大于 0
     * @return 发起第retries次重试前延迟的时间（单位毫秒）
     */
    public long getPauseDelay(String action, Exception ex, int retries);
}
