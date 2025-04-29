package com.alicloud.openservices.tablestore.timeserieswriter.callback;

public class TimeseriesRowResult {
    /**
     * To be further integrated with the billing interface
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
