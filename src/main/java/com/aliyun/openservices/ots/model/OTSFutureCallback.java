package com.aliyun.openservices.ots.model;

public interface OTSFutureCallback<T> {

    void completed(T result);

    void failed(Exception ex);

}