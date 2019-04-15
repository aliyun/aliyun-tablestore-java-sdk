package com.alicloud.openservices.tablestore.timestream.model.query;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import com.alicloud.openservices.tablestore.timestream.model.*;
import com.alicloud.openservices.tablestore.timestream.model.TimeRange;
import com.alicloud.openservices.tablestore.timestream.model.filter.Filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DataQuery {
    private AsyncClient asyncClient;
    private String tableName;
    private List<String> columnToGet = new ArrayList<String>();
    private TimeRange timeRange;
    private long timestamp = -1;

    public DataQuery(AsyncClient asyncClient, String tableName) {
        this.asyncClient = asyncClient;
        this.tableName = tableName;
    }

    protected void setColumnToGet(String... columns) {
        this.columnToGet = Arrays.asList(columns);
    }

    protected void setTimeRange(TimeRange timeRange) {
        if (timestamp != -1) {
            throw new ClientException("The timestamp has been set.");
        }
        this.timeRange = timeRange;
    }

    protected void setTimestamp(long timestamp, TimeUnit unit) {
        if (this.timeRange != null) {
            throw new ClientException("time range has been set");
        }
        if (timestamp < 0) {
            throw new ClientException("timestamp must be positive");
        }
        this.timestamp = unit.toMicros(timestamp);
    }

    private  PointIterator getTimestreamWithRange(TimestreamIdentifier identifier) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);

        long start = 0;
        long end = Long.MAX_VALUE;
        if (timeRange != null) {
            start = timeRange.getBeginTime();
            end = timeRange.getEndTime();
        }
        PrimaryKeyBuilder beginPk = Utils.convertIdentifierToPK(identifier);
        beginPk.addPrimaryKeyColumn(
                TableMetaGenerator.CN_TAMESTAMP_NAME,
                PrimaryKeyValue.fromLong(start)
        );

        PrimaryKeyBuilder endPk = Utils.convertIdentifierToPK(identifier);
        endPk.addPrimaryKeyColumn(
                TableMetaGenerator.CN_TAMESTAMP_NAME,
                PrimaryKeyValue.fromLong(end)
        );

        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(beginPk.build());
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPk.build());
        rangeRowQueryCriteria.setMaxVersions(1);
        rangeRowQueryCriteria.addColumnsToGet(columnToGet);
        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(rangeRowQueryCriteria);

        return new PointIterator(new GetRangeIterator(asyncClient, request), identifier);
    }

    private  PointIterator getTimestreamWithTimestamp(TimestreamIdentifier identifier) {
        SingleRowQueryCriteria singleRowQueryCriteria = new SingleRowQueryCriteria(tableName);
        PrimaryKeyBuilder pkBuilder = Utils.convertIdentifierToPK(identifier);
        pkBuilder.addPrimaryKeyColumn(
                TableMetaGenerator.CN_TAMESTAMP_NAME,
                PrimaryKeyValue.fromLong(timestamp)
        );
        singleRowQueryCriteria.setPrimaryKey(pkBuilder.build());
        singleRowQueryCriteria.setMaxVersions(1);
        singleRowQueryCriteria.addColumnsToGet(columnToGet);
        GetRowRequest request = new GetRowRequest(singleRowQueryCriteria);
        return new PointIterator(new GetRowIterator(asyncClient, request), identifier);
    }

    protected PointIterator getTimestream(TimestreamIdentifier identifier) {
        if (timestamp == -1) {
            return getTimestreamWithRange(identifier);
        } else {
            return getTimestreamWithTimestamp(identifier);
        }
    }

    public List<String> getSelectColumn() {
        return columnToGet;
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
