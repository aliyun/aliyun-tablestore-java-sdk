package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


/**
 * 单行的数据变更操作的基础结构。
 * <p>若是PutRow操作，请参考{@link RowPutChange}。</p>
 * <p>若是UpdateRow操作，请参考{@link RowUpdateChange}。</p>
 * <p>若是DeleteRow操作，请参考{@link RowDeleteChange}。</p>
 */
public abstract class RowChange implements IRow, Measurable {
    /**
     * 表的名称。
     */
    private String tableName;
    
    /**
     * 表的主键。
     */
    private PrimaryKey primaryKey;
    
    /**
     * 判断条件。
     */
    private Condition condition;
    
    /**
     * 返回的数据类型，默认是不返回。
     */
    private ReturnType returnType;

    /**
     * 指定本次修改需要返回列值的列名，支持修改类型：原子加。
     */
    private Set<String> returnColumnNames = new HashSet<String>();

    /**
     * 构造函数。
     * internal use
     * <p>表的名称不能为null或者为空。</p>
     * <p>行的主键不能为null或者为空。</p>
     *
     * @param tableName 表的名称
     * @param primaryKey 表的主键
     */
    public RowChange(String tableName, PrimaryKey primaryKey){
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The name of table should not be null or empty.");
        Preconditions.checkArgument(primaryKey != null && !primaryKey.isEmpty(), "The primary key of row should not be null or empty.");
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.condition = new Condition();
        this.returnType = ReturnType.RT_NONE;
    }
    
    /**
     * 构造函数。
     * internal use
     * <p>表的名称不能为null或者为空。</p>
     *
     * @param tableName 表的名称
     */
    public RowChange(String tableName) {
    	Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The name of table should not be null or empty.");
        this.tableName = tableName;
        this.condition = new Condition();
        this.returnType = ReturnType.RT_NONE;
    }
    
    /**
     *  设置表的名称。
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    /**
     *  获取表的名称。
     *
     * @return 表的名称
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 添加主键（Primary Key）列的名称和值。
     * @param primaryKey 行的主键。
     */
    public void setPrimaryKey(PrimaryKey primaryKey){
        Preconditions.checkNotNull(primaryKey, "primaryKey");

        this.primaryKey = primaryKey;
    }

    /**
     * 获取该行的主键。
     *
     * @return 行的主键
     */
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    /**
     * 获取判断条件。
     * @return 判断条件。
     */
    public Condition getCondition() {
        return condition;
    }

    /**
     * 设置判断条件。
     * @param condition 判断条件。
     */
    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    @Override
    public int compareTo(IRow row) {
        return this.primaryKey.compareTo(row.getPrimaryKey());
    }

    public ReturnType getReturnType() {
        return returnType;
    }

    public void setReturnType(ReturnType returnType) {
        this.returnType = returnType;
    }

    public void addReturnColumn(String columnName) {
        Preconditions.checkArgument(columnName != null && !columnName.isEmpty(), "Column's name should not be null or empty.");
        this.returnColumnNames.add(columnName);
    }

    public Set<String> getReturnColumnNames() {
        return Collections.unmodifiableSet(returnColumnNames);
    }

}
