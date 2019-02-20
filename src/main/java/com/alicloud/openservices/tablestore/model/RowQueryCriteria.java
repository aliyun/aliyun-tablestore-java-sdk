package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.model.filter.Filter;

import java.util.*;

/**
 * 从TableStore内读取数据操作的基本参数，主要包含：
 * <ul>
 * <li>ColumnsToGet: 要读取的属性列名列表，若为空，则代表读取该行所有的列。</li>
 * <li>TimeRange: 要读取的时间戳的范围，若未设置，则代表读取所有的版本。</li>
 * <li>MaxVersions: 要返回的列的版本的个数，若未设置，则返回OTS当前保留的所有版本。</li>
 * <li>Filter: 代表本次查询使用的Filter，Filter能够对查询范围内的数据在服务端进行初步的过滤，以减少额外的传输数据。</li>
 * </ul>
 */
public class RowQueryCriteria {
    /**
     * 查询的表的名称。
     */
    private String tableName;

    /**
     * 要读取的属性列名列表，若为空，则代表读取该行所有的列。
     */
    private Set<String> columnsToGet = new HashSet<String>();

    /**
     * 要读取的时间戳的范围，若未设置，则代表读取所有的版本。
     */
    private OptionalValue<TimeRange> timeRange = new OptionalValue<TimeRange>("TimeRange");

    /**
     * 要返回的列的版本的个数，若未设置，则返回OTS当前保留的所有版本。
     */
    private OptionalValue<Integer> maxVersions = new OptionalValue<Integer>("MaxVersions");

    /**
     * 本次查询使用的Filter。
     */
    private OptionalValue<Filter> filter = new OptionalValue<Filter>("Filter");

    /**
     * 查询的列范围的起始位置.
     */
    private OptionalValue<String> startColumn = new OptionalValue<String>("StartColumn");

    /**
     * 查询的列范围的终止位置.
     */
    private OptionalValue<String> endColumn = new OptionalValue<String>("EndColumn");


    /**
     * 内部参数。
     */
    private OptionalValue<Boolean> cacheBlocks = new OptionalValue<Boolean>("CacheBlocks");

    /**
     * 构造函数。
     *
     * @param tableName 查询的表名
     */
    public RowQueryCriteria(String tableName) {
        Preconditions.checkArgument(
        		tableName != null && !tableName.isEmpty(), 
        		"The name of table should not be null or empty.");
        this.tableName = tableName;
    }

    /**
     * 设置查询的表名。
     *
     * @param tableName 表的名称。
     */
    public void setTableName(String tableName) {
    	Preconditions.checkArgument(
    			tableName != null && !tableName.isEmpty(), 
    			"The name of table should not be null or empty.");
    	this.tableName = tableName;
    }

    /**
     * 返回查询的表名。
     *
     * @return 表的名称
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 返回要读取的列的名称列表（只读）。
     *
     * @return 列的名称的列表（只读）。
     */
    public Set<String> getColumnsToGet() {
        return Collections.unmodifiableSet(columnsToGet);
    }

    /**
     * 添加要读取的列。
     *
     * @param columnName 要返回列的名称。
     */
    public void addColumnsToGet(String columnName) {
        Preconditions.checkArgument(columnName != null && !columnName.isEmpty(), "Column's name should not be null or empty.");
        this.columnsToGet.add(columnName);
    }

    /**
     * 添加要读取的列。
     *
     * @param columnNames 要返回列的名称。
     */
    public void addColumnsToGet(String[] columnNames) {
        Preconditions.checkNotNull(columnNames, "columnNames should not be null.");
        for (int i = 0; i < columnNames.length; ++i) {
            addColumnsToGet(columnNames[i]);
        }
    }

    /**
     * 添加要读取的列。
     *
     * @param columnsToGet
     */
    public void addColumnsToGet(Collection<String> columnsToGet) {
        this.columnsToGet.addAll(columnsToGet);
    }

    /**
     * 将设置过的要读取的列的名称列表清空。
     */
    public void clearColumnsToGet() {
        this.columnsToGet.clear();
    }

    /**
     * 返回要读取的列的个数。
     *
     * @return 要读取的列的个数。
     */
    public int numColumnsToGet() {
        return this.columnsToGet.size();
    }

    /**
     * 设置MaxVersions。
     *
     * @param maxVersions
     */
    public void setMaxVersions(int maxVersions) {
        Preconditions.checkArgument(maxVersions > 0, "The value of maxVersions must be greater than 0.");
        this.maxVersions.setValue(maxVersions);
    }

    /**
     * 获取设置过的MaxVersions。
     *
     * @return MaxVersions
     * @throws java.lang.IllegalStateException 若没有配置该参数
     */
    public int getMaxVersions() {
        if (!this.maxVersions.isValueSet()) {
            throw new IllegalStateException("The value of maxVersions is not set.");
        }
        return this.maxVersions.getValue();
    }

    /**
     * 查询是否设置了MaxVersions。
     *
     * @return 若设置过MaxVersions，则返回true，否则返回false。
     */
    public boolean hasSetMaxVersions() {
        return this.maxVersions.isValueSet();
    }

    /**
     * 设置要读取的时间戳范围。
     *
     * @param timeRange 时间戳范围
     */
    public void setTimeRange(TimeRange timeRange) {
        Preconditions.checkNotNull(timeRange, "The time range should not be null.");
        this.timeRange.setValue(timeRange);
    }

    /**
     * 设置要读取的某个特定时间戳。
     *
     * @param timestamp 时间戳
     */
    public void setTimestamp(long timestamp) {
        Preconditions.checkArgument(timestamp >= 0, "The timestamp must be positive.");
        this.timeRange.setValue(new TimeRange(timestamp, timestamp + 1));
    }

    /**
     * 获取设置过的时间戳范围。
     *
     * @return TimeRange
     * @throws java.lang.IllegalStateException 若没有配置该参数
     */
    public TimeRange getTimeRange() {
        if (!this.timeRange.isValueSet()) {
            throw new IllegalStateException("The value of timeRange is not set.");
        }
        return this.timeRange.getValue();
    }

    /**
     * 查询是否设置过TimeRange。
     *
     * @return 若设置过TimeRange，则返回true，否则返回false
     */
    public boolean hasSetTimeRange() {
        return this.timeRange.isValueSet();
    }

    /**
     * 设置本次查询使用的Filter。
     *
     * @param filter
     */
    public void setFilter(Filter filter) {
        Preconditions.checkNotNull(filter, "The filter should not be null");
        this.filter.setValue(filter);
    }

    /**
     * 获取本次查询使用的Filter。
     *
     * @return Filter
     * @throws java.lang.IllegalStateException 若没有设置Filter
     */
    public Filter getFilter() {
        if (!this.filter.isValueSet()) {
            throw new IllegalStateException("The value of filter is not set.");
        }
        return this.filter.getValue();
    }

    /**
     * 查询是否设置了Filter。
     *
     * @return 若设置了Filter，则返回true，否则返回false。
     */
    public boolean hasSetFilter() {
        return this.filter.isValueSet();
    }


    /**
     * 设置本次读操作返回数据是否要进BlockCache。
     *
     * @param cacheBlocks 若为true，读取的数据会进入BlockCache
     */
    public void setCacheBlocks(boolean cacheBlocks) {
        this.cacheBlocks.setValue(cacheBlocks);
    }

    /**
     * 获取CacheBlocks的设置的值。
     *
     * @return CacheBlocks
     * @throws java.lang.IllegalStateException 若没有配置该参数
     */
    public boolean getCacheBlocks() {
        if (!this.cacheBlocks.isValueSet()) {
            throw new IllegalStateException("The value of cacheBlocks is not set.");
        }
        return this.cacheBlocks.getValue();
    }

    /**
     * 查询是否设置了CacheBlocks。
     *
     * @return 若设置了CacheBlocks，则返回true，否则返回false。
     */
    public boolean hasSetCacheBlock() {
        return this.cacheBlocks.isValueSet();
    }

    public String getStartColumn() {
        if (!this.startColumn.isValueSet()) {
            throw new IllegalStateException("The value of startColumn is not set.");
        }
        return startColumn.getValue();
    }

    public void setStartColumn(String startColumn) {
        this.startColumn.setValue(startColumn);
    }

    public boolean hasSetStartColumn() {
        return this.startColumn.isValueSet();
    }

    public String getEndColumn() {
        if (!this.endColumn.isValueSet()) {
            throw new IllegalStateException("The value of endColumn is not set.");
        }
        return endColumn.getValue();
    }

    public void setEndColumn(String endColumn) {
        this.endColumn.setValue(endColumn);
    }

    public boolean hasSetEndColumn() {
        return this.endColumn.isValueSet();
    }

    public void copyTo(RowQueryCriteria target) {
        target.tableName = tableName;
        target.columnsToGet.addAll(columnsToGet);
        if (timeRange.isValueSet()) {
            target.timeRange.setValue(timeRange.getValue());
        }

        if (maxVersions.isValueSet()) {
            target.maxVersions.setValue(maxVersions.getValue());
        }

        if (cacheBlocks.isValueSet()) {
            target.cacheBlocks.setValue(cacheBlocks.getValue());
        }

        if (filter.isValueSet()) {
            target.filter.setValue(filter.getValue());
        }

        if (startColumn.isValueSet()) {
            target.startColumn.setValue(startColumn.getValue());
        }

        if (endColumn.isValueSet()) {
            target.endColumn.setValue(endColumn.getValue());
        }
    }
}
