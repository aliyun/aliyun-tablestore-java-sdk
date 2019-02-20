package com.alicloud.openservices.tablestore.model;

public interface RetryStrategy {

    /**
     * 返回一个同类型且尚未重试的RetryStrategy对象。
     *
     * @return 同类型且尚未重试的RetryStrategy对象。
     */
    public RetryStrategy clone();
    
    /**
     * 返回当前重试的次数
     *
     * @return 当前重试的次数
     */
    public int getRetries();

    /**
     * 得到发起第retries次重试前延迟的时间。SDK会在这一段时间之后发起第retries次重试。
     * 若返回值小于等于0, 表示不重试.
     *
     * @param action  操作名，比如"ListTable"、"GetRow"、"PutRow"等
     * @param ex      上次访问失败的错误信息、为ClientException或TableStoreException
     * @return 发起第retries次重试前延迟的时间（单位毫秒）。小于等于0表示不可重试。
     */
    public long nextPause(String action, Exception ex);
}
