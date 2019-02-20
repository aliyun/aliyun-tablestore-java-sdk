package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class RangeRowQueryCriteria extends RowQueryCriteria {
    
    private Direction direction = Direction.FORWARD;
    
    /**
     * 返回的最大行数。
     */
    private int limit = -1;
    
    private PrimaryKey inclusiveStartPrimaryKey;
    
    private PrimaryKey exclusiveEndPrimaryKey;

    /**
     * 用于行内流式读, 标记位置和状态信息.
     */
    private OptionalValue<byte[]> token = new OptionalValue<byte[]>("Token");


    /**
     * 构造一个在给定名称的表中查询的条件。
     * @param tableName
     *          查询的表名。
     */
    public RangeRowQueryCriteria(String tableName){
        super(tableName);
    }

    /**
     * 获取操作时返回的最大行数。
     * -1 表示返回该返回内的所有行（默认值）。
     * @return 本次操作返回的最大行数。
     */
    public int getLimit() {
        return limit;
    }

    /**
     * 设置查询时单次请求返回的行数。
     * -1 表示全部符合条件的数据行（默认值）。
     * @param limit 单次请求返回的行数。
     */
    public void setLimit(int limit) {
        Preconditions.checkArgument(limit > 0, "The limit must be greater than 0.");
        this.limit = limit;
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
        Preconditions.checkNotNull(direction, "The direction should not be null.");
        this.direction = direction;
    }
    
    /**
     * 获取范围查询的左边界的主键值。
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
     * @param inclusiveStartPrimaryKey 范围查询的左边界的主键值。
     */
    public void setInclusiveStartPrimaryKey(PrimaryKey inclusiveStartPrimaryKey) {
        Preconditions.checkArgument(inclusiveStartPrimaryKey != null && !inclusiveStartPrimaryKey.isEmpty(), "The inclusive start primary key should not be null.");
        this.inclusiveStartPrimaryKey = inclusiveStartPrimaryKey;
    }

    /**
     * 获取范围查询的右边界的主键值。
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
     * @param exclusiveEndPrimaryKey 范围查询的右边界的主键值。
     */
    public void setExclusiveEndPrimaryKey(PrimaryKey exclusiveEndPrimaryKey) {
        Preconditions.checkArgument(exclusiveEndPrimaryKey != null && !exclusiveEndPrimaryKey.isEmpty(), "The exclusive end primary key should not be null.");
        this.exclusiveEndPrimaryKey = exclusiveEndPrimaryKey;
    }

    public byte[] getToken() {
        if (!this.token.isValueSet()) {
            throw new IllegalStateException("The value of token is not set.");
        }
        return token.getValue();
    }

    public void setToken(byte[] token) {
        if (token != null) {
            this.token.setValue(token);
        }
    }

    public boolean hasSetToken() {
        return this.token.isValueSet();
    }
}
