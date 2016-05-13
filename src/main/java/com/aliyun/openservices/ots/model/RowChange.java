package com.aliyun.openservices.ots.model;

import com.aliyun.openservices.ots.utils.CodingUtils;


/**
 * 表示行的数据变更信息。
 *
 */
public abstract class RowChange {
    /**
     * 表的名称。
     */
    private String tableName;
    
    /**
     * 表的主键集合。
     */
    protected RowPrimaryKey primaryKey = new RowPrimaryKey();
    
    /**
     * 判断条件。
     */
    private Condition condition;

    /**
     * 构造函数。
     */
    protected RowChange(String tableName){
        setTableName(tableName);
        condition = new Condition();
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    /**
     * 获取主键（Primary Key）列名称与值的对应只读字典。
     * @return 主键（Primary Key）列名称与值的对应只读字典。
     */
    public RowPrimaryKey getRowPrimaryKey() {
        return primaryKey;
    }

    /**
     * 添加主键（Primary Key）列的名称和值。
     * @param primaryKey 行的主键。
     */
    public void setPrimaryKey(RowPrimaryKey primaryKey){
        CodingUtils.assertParameterNotNull(primaryKey, "primaryKey");

        this.primaryKey = primaryKey;
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

    /**
     * 获取一行的数据大小。
     * <p/>
     * RowPutChange: 一行的数据大小包括所有主键列的名称和值的大小，以及所有属性列名称和值的大小总和。<br/>
     * RowUpdateChange: 一行的数据大小包括所有主键列的名称和值的大小，以及所有需要被更改的属性列的名称和值的大小，以及所有需要被删除的属性列的名称大小总和。<br/>
     * RowDeleteChange: 一行的数据大小只包括所有主键列的名称和值的大小总和。<br/>
     *
     * @return
     */
    public abstract int getDataSize();
}
