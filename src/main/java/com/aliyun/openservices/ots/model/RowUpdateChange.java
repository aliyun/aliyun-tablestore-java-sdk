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
public class RowUpdateChange extends RowChange {
    /**
     * 需要更改的属性列的值。
     * 若属性列的值为Null，则代表删除该列。
     */
    private Map<String, ColumnValue> attributeColumns = 
            new HashMap<String, ColumnValue>();

    private int valueTotalSize = -1;

    /**
     * 构造一个新的{@link RowPutChange}实例。
     * @param tableName 表的名称
     */
    public RowUpdateChange(String tableName){
        super(tableName);
    }

    @Override
    public int getDataSize() {
        if (valueTotalSize == -1) {
            valueTotalSize = 0;
            for (Map.Entry<String, ColumnValue> entry : attributeColumns.entrySet()) {
                valueTotalSize += CalculateHelper.getStringDataSize(entry.getKey());
                valueTotalSize += entry.getValue() == null ? 0 : entry.getValue().getSize();
            }
        }
        return primaryKey.getSize() + valueTotalSize;
    }

    /**
     * 返回属性列的名称与值的对应字典（只读）。
     * 若获取到得属性列的值为Null，则代表删除该列。
     * @return 名称与值的对应字典（只读）。
     */
    public Map<String, ColumnValue> getAttributeColumns() {
        return Collections.unmodifiableMap(attributeColumns);
    }

    /**
     * 添加属性列的名称和值。
     * @param name 属性列的列名。
     * @param value 属性列的值。
     */
    public void addAttributeColumn(String name, ColumnValue value) {
        CodingUtils.assertParameterNotNull(name, "name");
        CodingUtils.assertParameterNotNull(value, "value");
        this.attributeColumns.put(name, value);
        this.valueTotalSize = -1; // value changed, reset size
    }
    
    /**
     * 删除某个属性列。
     * @param name 属性列的名称。
     */
    public void deleteAttributeColumn(String name) {
        this.attributeColumns.put(name, null);
        this.valueTotalSize = -1; // value changed, reset size
    }
}
