package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class QueryTimeseriesMetaRequest implements Request {

    private final String timeseriesTableName;
    private MetaQueryCondition condition;
    private boolean getTotalHits = false;
    private int limit = -1;
    private byte[] nextToken;

    public QueryTimeseriesMetaRequest(String timeseriesTableName) {
        Preconditions.checkNotNull(timeseriesTableName);
        this.timeseriesTableName = timeseriesTableName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_QUERY_TIMESERIES_META;
    }

    public MetaQueryCondition getCondition() {
        return condition;
    }

    public void setCondition(MetaQueryCondition condition) {
        this.condition = condition;
    }

    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    public boolean isGetTotalHits() {
        return getTotalHits;
    }

    public void setGetTotalHits(boolean getTotalHits) {
        this.getTotalHits = getTotalHits;
    }

    public byte[] getNextToken() {
        return nextToken;
    }

    public void setNextToken(byte[] nextToken) {
        this.nextToken = nextToken;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }
}
