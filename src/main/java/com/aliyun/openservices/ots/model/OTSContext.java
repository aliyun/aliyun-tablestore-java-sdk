package com.aliyun.openservices.ots.model;

public class OTSContext<Req, Res> {
    private Req otsRequest;
    private Res otsResult;

    public OTSContext(Req otsRequest, Res otsResult) {
        this.otsRequest = otsRequest;
        this.otsResult = otsResult;
    }

    public Req getOTSRequest() {
        return otsRequest;
    }

    public void setOTSRequest(Req otsRequest) {
        this.otsRequest = otsRequest;
    }

    public Res getOTSResult() {
        return otsResult;
    }

    public void setOTSResult(Res otsResult) {
        this.otsResult = otsResult;
    }
}
