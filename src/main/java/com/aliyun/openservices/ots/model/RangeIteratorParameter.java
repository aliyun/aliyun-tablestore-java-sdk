package com.aliyun.openservices.ots.model;

import static com.aliyun.openservices.ots.utils.CodingUtils.*;

/**
 * 表示获取表（Table）中主键（Primary Key）的特定范围内多行数据的查询条件。
 *
 */
public class RangeIteratorParameter extends RowQueryCriteria {
    
    private Direction direction = Direction.FORWARD;
    
    /**
     * 此次Iterator查询返回的最大行数，若count未设置，则返回查询范围下的所有行。
     * 默认-1代表不对行数做限制，读取该范围下所有行。
     */
    private int count = -1;
    
    /**
     * Iterator分批查询时用到的buffer的大小，该大小决定了Iterator调用GetRange查询时每次请求返回的最大行数。
     * 默认-1代表不设置buffer的大小，每次按OTS一次请求最多返回行数来。
     */
    private int bufferSize = -1;
    
    private RowPrimaryKey inclusiveStartPrimaryKey = new RowPrimaryKey();
    
    private RowPrimaryKey exclusiveEndPrimaryKey = new RowPrimaryKey();
   
    /**
     * 构造一个在给定名称的表中查询的条件。
     * @param tableName
     *          查询的表名。
     */
    public RangeIteratorParameter(String tableName){
        super(tableName);
    }

    /**
     * 获取操作时返回的最大行数。
     * -1 表示返回该范围内的所有行（默认值）。
     * @return 本次操作返回的最大行数。
     */
    public int getCount() {
        return count;
    }

    /**
     * 设置查询时单次请求返回的行数。
     * -1 表示全部符合条件的数据行（默认值）。
     * @param count 单次请求返回的行数。
     */
    public void setCount(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("Count must be greater than 0.");
        }
        this.count = count;
    }

    /**
     * 获取内部Buffer的大小。
     * @return Buffer的大小。
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * 设置Buffer的大小。
     * @param bufferSize Buffer的大小。
     */
    public void setBufferSize(int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("Buffer size must be greater than 0.");
        }
        this.bufferSize = bufferSize;
    }

    /**
     * 获取范围查询的读取顺序（正序(FORWARD)或反序(BACKWARD)）。
     * @return 读取顺序
     */
    public Direction getDirection() {
        return direction;
    }

    /**
     * 设置范围查询的读取顺序（正序(FORWARD)或反序(BACKWARD)）。
     * @param direction 读取顺序
     */
    public void setDirection(Direction direction) {
        assertParameterNotNull(direction, "direction");
        this.direction = direction;
    }
    
    /**
     * 获取范围查询的左边界的主键值。
     * @return 范围查询的左边界的主键值。
     */
    public RowPrimaryKey getInclusiveStartPrimaryKey() {
        return inclusiveStartPrimaryKey;
    }

    /**
     * 范围查询需要用户指定一个主键的范围，该范围是一个左闭右开的区间，inclusiveStartPrimaryKey为该区间的左边界。
     * 若direction为FORWARD，则inclusiveStartPrimaryKey必须小于exclusiveEndPrimaryKey。
     * 若direction为BACKWARD，则inclusiveStartPrimaryKey必须大于exclusiveEndPrimaryKey。
     * inclusiveStartPrimaryKey必须包含表中定义的所有主键列，列的值可以定义PrimaryKeyRange.INF_MIN或者PrimaryKeyRange.INF_MAX用于表示该列的所有取值范围。
     * @param inclusiveStartPrimaryKey 范围查询的左边界的主键值。
     */
    public void setInclusiveStartPrimaryKey(RowPrimaryKey inclusiveStartPrimaryKey) {
        assertParameterNotNull(inclusiveStartPrimaryKey, "inclusiveStartPrimaryKey");
        this.inclusiveStartPrimaryKey = inclusiveStartPrimaryKey;
    }

    /**
     * 获取范围查询的右边界的主键值。
     * @return 范围查询的右边界的主键值。
     */
    public RowPrimaryKey getExclusiveEndPrimaryKey() {
        return exclusiveEndPrimaryKey;
    }

    /**
     * 范围查询需要用户指定一个主键的范围，该范围是一个左闭右开的区间，exclusiveEndPrimaryKey为该区间的右边界。
     * 若direction为FORWARD，则exclusiveEndPrimaryKey必须大于inclusiveStartPrimaryKey。
     * 若direction为BACKWARD，则exclusiveEndPrimaryKey必须小于inclusiveStartPrimaryKey。
     * exclusiveEndPrimaryKey必须包含表中定义的所有主键列，列的值可以定义PrimaryKeyRange.INF_MIN或者PrimaryKeyRange.INF_MAX用于表示该列的所有取值范围。
     * @param exclusiveEndPrimaryKey 范围查询的右边界的主键值。
     */
    public void setExclusiveEndPrimaryKey(RowPrimaryKey exclusiveEndPrimaryKey) {
        assertParameterNotNull(exclusiveEndPrimaryKey, "exclusiveEndPrimaryKey");
        this.exclusiveEndPrimaryKey = exclusiveEndPrimaryKey;
    }
}
