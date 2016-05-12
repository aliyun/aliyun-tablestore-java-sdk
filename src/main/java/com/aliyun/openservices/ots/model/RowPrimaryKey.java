/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 *
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

import com.aliyun.openservices.ots.utils.CalculateHelper;

import static com.aliyun.openservices.ots.utils.CodingUtils.assertParameterNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RowPrimaryKey {
    private Map<String, PrimaryKeyValue> primaryKey = new HashMap<String, PrimaryKeyValue>();

    private int dataSize = -1;

    public RowPrimaryKey(){
    }

    /**
     * 获取主键（Primary Key）列名称与值的对应字典（只读）。
     * @return 主键（Primary Key）列名称与值的对应字典（只读）。
     */
    public Map<String, PrimaryKeyValue> getPrimaryKey() {
        return Collections.unmodifiableMap(primaryKey);
    }

    /**
     * 添加主键（Primary Key）列的名称和值。
     * @param name 主键列的名称。
     * @param value 主键列的值。
     * @return this for chain invocation
     */
    public RowPrimaryKey addPrimaryKeyColumn(String name, PrimaryKeyValue value) {
        assertParameterNotNull(name, "name");
        assertParameterNotNull(value, "value");

        this.primaryKey.put(name, value);
        this.dataSize = -1; // value changed, reset size
        return this;
    }

    /**
     * 获取行主键的数据大小总和，大小总和包括所有主键列的名称和值。
     *
     * @return 行主键的数据大小总和
     */
    public int getSize() {
        if (dataSize == -1) {
            dataSize = 0;
            for (Map.Entry<String, PrimaryKeyValue> entry : primaryKey.entrySet()) {
                this.dataSize += CalculateHelper.getStringDataSize(entry.getKey());
                this.dataSize += entry.getValue().getSize();
            }
        }
        return dataSize;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof RowPrimaryKey)) {
            return false;
        }

        RowPrimaryKey pk = (RowPrimaryKey) o;
        if (this.primaryKey.size() != pk.primaryKey.size()) {
            return false;
        }

        for (Map.Entry<String, PrimaryKeyValue> entry : primaryKey.entrySet()) {
            PrimaryKeyValue value = pk.primaryKey.get(entry.getKey());
            if (value == null) {
                return false;
            }
            if (!value.equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 1;

        for (Map.Entry<String, PrimaryKeyValue> entry : primaryKey.entrySet()) {
            result = 31 * result + entry.getKey().hashCode();
            result = 31 * result + entry.getValue().hashCode();
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, PrimaryKeyValue> pk : this.primaryKey.entrySet()) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(pk.getKey());
            sb.append(":");
            sb.append(pk.getValue());
        }
        return sb.toString();
    }
}
