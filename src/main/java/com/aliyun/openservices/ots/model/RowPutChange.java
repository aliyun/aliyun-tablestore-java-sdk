package com.aliyun.openservices.ots.model;

import com.aliyun.openservices.ots.utils.CalculateHelper;
import com.aliyun.openservices.ots.utils.CodingUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * 表示行的插入或更新信息。
 *
 */
public class RowPutChange extends RowChange{
    /**
     * 行的属性列集合。
     */
    private Map<String, ColumnValue> attributeColumns = 
            new HashMap<String, ColumnValue>();

    private int valueTotalSize = -1;
    /**
     * 构造一个新的{@link RowPutChange}实例。
     * @param tableName 表的名称
     */
    public RowPutChange(String tableName){
        super(tableName);
    }

    @Override
    public int getDataSize() {
        if (valueTotalSize == -1) {
            valueTotalSize = 0;
            for (Map.Entry<String, ColumnValue> entry : attributeColumns.entrySet()) {
                valueTotalSize += CalculateHelper.getStringDataSize(entry.getKey());
                valueTotalSize += entry.getValue().getSize();
            }
        }
        return primaryKey.getSize() + valueTotalSize;
    }

    /**
     * 返回属性列的名称与值的对应字典（只读）。
     * @return 名称与值的对应字典（只读）。
     */
    public Map<String, ColumnValue> getAttributeColumns(){
        return Collections.unmodifiableMap(attributeColumns);
    }

    /**
     * 添加属性列的名称和值。
     * @param name 属性列的列名。
     * @param value 属性列的值。
     * @return this for chain invocation
     */
    public RowPutChange addAttributeColumn(String name, ColumnValue value){
        CodingUtils.assertParameterNotNull(name, "name");
        CodingUtils.assertParameterNotNull(value, "value");
        this.attributeColumns.put(name, value);
        this.valueTotalSize = -1; // value changed, reset size
        return this;
    }
}
