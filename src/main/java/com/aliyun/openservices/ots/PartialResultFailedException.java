package com.aliyun.openservices.ots;

import com.aliyun.openservices.ots.model.OTSResult;

import java.util.ArrayList;
import java.util.List;

public class PartialResultFailedException extends OTSException {

    private OTSResult otsResult;
    private List<OTSException> errors = new ArrayList<OTSException>();

    /**
     * 构造函数。
     *
     * @param cause      错误原因
     * @param requestId  RequestId
     */
    public PartialResultFailedException(Throwable cause, String requestId, OTSResult otsResult) {
        super("Partial result failed in batch operations.", cause, "", requestId, 0);
        this.otsResult = otsResult;
    }

    public void addError(OTSException ex) {
        errors.add(ex);
    }

    public List<OTSException> getErrors() {
        return this.errors;
    }

    public OTSResult getResult() {
        return this.otsResult;
    }

}
