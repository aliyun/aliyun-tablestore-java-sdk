package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * DeleteDefinedColumn包含为一张表删除预定义列所必需的一些参数，包括表的名字，要删除的预定义列的名字
 */
public class DeleteDefinedColumnRequest implements Request {
    private String tableName;
    private List<String> definedColumns = new ArrayList<String>();

    /**
     * 设置表的名称。
     *
     * @param tableName 表的名称。
     */
    public void setTableName(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The name of table should not be null or empty.");

        this.tableName = tableName;
    }

    /**
     * 获取表名称
     * @return 表的名称
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 添加一个预定义列
     * @param name 预定义列的名称。
     */
    public void addDefinedColumn(String name) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of defined column should not be null or empty.");

        this.definedColumns.add(name);
    }

    /**
     * 获取预定义列列表
     * @return 预定义列列表
     */
    public List<String> getDefinedColumn() {
        return Collections.unmodifiableList(definedColumns);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_DEFINED_COLUMN;
    }
}

