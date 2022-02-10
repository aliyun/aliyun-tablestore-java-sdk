package com.alicloud.openservices.tablestore.timestream.model.query;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.alicloud.openservices.tablestore.timestream.model.*;

import java.util.concurrent.TimeUnit;

/**
 * 单条时间线的数据查询类
 */
public class DataGetter extends DataQuery {
    /**
     * 时间线标示
     */
    private TimestreamIdentifier identifier;

    public DataGetter(AsyncClient asyncClient, String tableName, TimestreamIdentifier identifier) {
        super(asyncClient, tableName);
        this.identifier = identifier;
    }

	/**
     * 设置数据行的过滤条件，仅支持对数据行的fields进行过滤
     * @param filter
     * @return
     */
    public DataGetter filter(Filter filter) {
        setFilter(filter);
        return this;
    }

    /**
     * 设置需要读取的field列表
     * @param fields 需要查询的数据字段
     * @return
     */
    public DataGetter select(String... fields) {
        setColumnToGet(fields);
        return this;
    }

    /**
     * 要读取的数据的时间戳范围
     * @param timeRange {@link TimeRange}，需要查询的数据时间范围
     * @return
     */
    public DataGetter timeRange(TimeRange timeRange) {
        setTimeRange(timeRange);
        return this;
    }

    /**
     * 要读取的数据点的时间戳
     * @param timestamp 需要查询的数据时间戳
     * @param unit 时间戳单位
     * @return
     */
    public DataGetter timestamp(long timestamp, TimeUnit unit) {
        setTimestamp(timestamp, unit);
        return this;
    }

	/**
     * 按照数据点的时间戳进行逆序排序，默认正序
     * @return
     */
    public DataGetter descTimestamp() {
        setOrderByTimestampDesc();
        return this;
    }

    /**
     * 设置查询时单次请求返回的行数
     * @param limit
     */
    public DataGetter limit(int limit) {
        setLimit(limit);
        return this;
    }

    /**
     * 查询
     * @return
     */
    public PointIterator fetchAll() {
        return getTimestream();
    }

    private PointIterator getTimestream() {
        return getTimestream(identifier);
    }
}
