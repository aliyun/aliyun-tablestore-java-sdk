package com.alicloud.openservices.tablestore.timestream.model.query;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.alicloud.openservices.tablestore.timestream.model.*;

import java.util.concurrent.TimeUnit;

/**
 * Data query class for a single timeline
 */
public class DataGetter extends DataQuery {
    /**
     * Timeline identifier
     */
    private TimestreamIdentifier identifier;

    public DataGetter(AsyncClient asyncClient, String tableName, TimestreamIdentifier identifier) {
        super(asyncClient, tableName);
        this.identifier = identifier;
    }

	/**
     * Set the filter condition for the data row, only supports filtering the fields of the data row
     * @param filter
     * @return
     */
    public DataGetter filter(Filter filter) {
        setFilter(filter);
        return this;
    }

    /**
     * Set the list of fields to be read
     * @param fields The data fields to be queried
     * @return
     */
    public DataGetter select(String... fields) {
        setColumnToGet(fields);
        return this;
    }

    /**
     * The timestamp range of the data to be read
     * @param timeRange {@link TimeRange}, the time range of the data to be queried
     * @return
     */
    public DataGetter timeRange(TimeRange timeRange) {
        setTimeRange(timeRange);
        return this;
    }

    /**
     * The timestamp of the data point to read
     * @param timestamp The timestamp of the data to query
     * @param unit The unit of the timestamp
     * @return
     */
    public DataGetter timestamp(long timestamp, TimeUnit unit) {
        setTimestamp(timestamp, unit);
        return this;
    }

	/**
     * Sort in reverse order by the timestamp of the data point, default is ascending.
     * @return
     */
    public DataGetter descTimestamp() {
        setOrderByTimestampDesc();
        return this;
    }

    /**
     * Set the number of rows returned in a single request during the query
     * @param limit
     */
    public DataGetter limit(int limit) {
        setLimit(limit);
        return this;
    }

    /**
     * Query
     * @return
     */
    public PointIterator fetchAll() {
        return getTimestream();
    }

    private PointIterator getTimestream() {
        return getTimestream(identifier);
    }
}
