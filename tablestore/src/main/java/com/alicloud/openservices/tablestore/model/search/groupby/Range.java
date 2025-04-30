package com.alicloud.openservices.tablestore.model.search.groupby;

public class Range {

    private Double from;

    private Double to;

    public Double getFrom() {
        return from;
    }

    public void setFrom(Double from) {
        this.from = from;
    }

    public Double getTo() {
        return to;
    }

    public void setTo(Double to) {
        this.to = to;
    }

    public Range(Double from, Double to) {
        this.from = from;
        this.to = to;
    }
}
