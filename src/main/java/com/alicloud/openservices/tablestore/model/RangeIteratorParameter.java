package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class RangeIteratorParameter extends RowQueryCriteria {

    private Direction direction = Direction.FORWARD;

    /**
     * 此次Iterator查询返回的最大行数，若count未设置，则返回查询范围下的所有行。
     * 默认-1代表不对行数做限制，读取该范围下所有行。
     */
    private int maxCount = -1;

    /**
     * Iterator分批查询时用到的buffer的大小，该大小决定了Iterator调用GetRange查询时每次请求返回的最大行数。
     * 默认-1代表不设置buffer的大小，每次按TableStore一次请求最多返回行数来。
     */
    private int bufferSize = -1;

    private PrimaryKey inclusiveStartPrimaryKey;

    private PrimaryKey exclusiveEndPrimaryKey;

    /**
     * 构造一个在给定名称的表中查询的条件。
     *
     * @param tableName 查询的表名。
     */
    public RangeIteratorParameter(String tableName) {
        super(tableName);
    }

    public RangeIteratorParameter(RangeRowQueryCriteria criteria) {
        super(criteria.getTableName());

        criteria.copyTo(this);
        this.direction = criteria.getDirection();
        this.bufferSize = criteria.getLimit();
        this.inclusiveStartPrimaryKey = criteria.getInclusiveStartPrimaryKey();
        this.exclusiveEndPrimaryKey = criteria.getExclusiveEndPrimaryKey();
    }

    /**
     * Iterator最多返回的行数。
     * -1 表示返回该范围内的所有行（默认值）。
     *
     * @return 本次操作返回的最大行数。
     */
    public int getMaxCount() {
        return maxCount;
    }

    /**
     * 设置该Iterator最多返回的行数。
     * -1 表示返回该范围内的所有行（默认值）
     *
     * @param maxCount 单次请求返回的行数。
     */
    public void setMaxCount(int maxCount) {
        Preconditions.checkArgument(maxCount > 0, "The max count must be greater than 0.");
        this.maxCount = maxCount;
    }

    /**
     * 获取内部Buffer的大小。
     *
     * @return Buffer的大小。
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * 设置Buffer的大小。
     *
     * @param bufferSize Buffer的大小。
     */
    public void setBufferSize(int bufferSize) {
        Preconditions.checkArgument(bufferSize > 0, "The buffer size must be greater than 0.");
        this.bufferSize = bufferSize;
    }

    /**
     * 获取范围查询的读取顺序（正序(FORWARD)或反序(BACKWARD)）。
     *
     * @return 读取顺序
     */
    public Direction getDirection() {
        return direction;
    }

    /**
     * 设置范围查询的读取顺序（正序(FORWARD)或反序(BACKWARD)）。
     *
     * @param direction 读取顺序
     */
    public void setDirection(Direction direction) {
        Preconditions.checkNotNull(direction, "The direction should not be null.");
        this.direction = direction;
    }

    /**
     * 获取范围查询的左边界的主键值。
     *
     * @return 范围查询的左边界的主键值。
     */
    public PrimaryKey getInclusiveStartPrimaryKey() {
        return inclusiveStartPrimaryKey;
    }

    /**
     * 范围查询需要用户指定一个主键的范围，该范围是一个左闭右开的区间，inclusiveStartPrimaryKey为该区间的左边界。
     * 若direction为FORWARD，则inclusiveStartPrimaryKey必须小于exclusiveEndPrimaryKey。
     * 若direction为BACKWARD，则inclusiveStartPrimaryKey必须大于exclusiveEndPrimaryKey。
     * inclusiveStartPrimaryKey必须包含表中定义的所有主键列，列的值可以定义{@link PrimaryKeyValue#INF_MIN}或者{@link PrimaryKeyValue#INF_MAX}用于表示该列的所有取值范围。
     *
     * @param inclusiveStartPrimaryKey 范围查询的左边界的主键值。
     */
    public void setInclusiveStartPrimaryKey(PrimaryKey inclusiveStartPrimaryKey) {
        Preconditions.checkArgument(inclusiveStartPrimaryKey != null && !inclusiveStartPrimaryKey.isEmpty(), "The start primary key should not be null or empty.");
        this.inclusiveStartPrimaryKey = inclusiveStartPrimaryKey;
    }

    /**
     * 获取范围查询的右边界的主键值。
     *
     * @return 范围查询的右边界的主键值。
     */
    public PrimaryKey getExclusiveEndPrimaryKey() {
        return exclusiveEndPrimaryKey;
    }

    /**
     * 范围查询需要用户指定一个主键的范围，该范围是一个左闭右开的区间，exclusiveEndPrimaryKey为该区间的右边界。
     * 若direction为FORWARD，则exclusiveEndPrimaryKey必须大于inclusiveStartPrimaryKey。
     * 若direction为BACKWARD，则exclusiveEndPrimaryKey必须小于inclusiveStartPrimaryKey。
     * exclusiveEndPrimaryKey必须包含表中定义的所有主键列，列的值可以定义{@link PrimaryKeyValue#INF_MIN}或者{@link PrimaryKeyValue#INF_MAX}用于表示该列的所有取值范围。
     *
     * @param exclusiveEndPrimaryKey 范围查询的右边界的主键值。
     */
    public void setExclusiveEndPrimaryKey(PrimaryKey exclusiveEndPrimaryKey) {
        Preconditions.checkArgument(exclusiveEndPrimaryKey != null && !exclusiveEndPrimaryKey.isEmpty(), "The end primary key should not be null or empty.");
        this.exclusiveEndPrimaryKey = exclusiveEndPrimaryKey;
    }
}
