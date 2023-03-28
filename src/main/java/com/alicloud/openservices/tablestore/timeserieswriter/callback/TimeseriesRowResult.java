package com.alicloud.openservices.tablestore.timeserieswriter.callback;

public class TimeseriesRowResult {
    /**
     * 待进一步接入计费接口
     * for example: {@link com.alicloud.openservices.tablestore.model.ConsumedCapacity}
     */
    boolean isSuccess;

    Error error;

    public TimeseriesRowResult(boolean isSuccess, Error error) {
        this.isSuccess = isSuccess;
        this.error = error;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(boolean success) {
        isSuccess = success;
    }

    public Error getError() {
        return error;
    }

    public void setError(Error error) {
        this.error = error;
    }
}
