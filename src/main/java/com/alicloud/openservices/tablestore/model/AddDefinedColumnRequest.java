package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * AddDefinedColumn包含为一张表添加预定义列所必需的一些参数，包括表的名字，要添加的预定义列的名字及类型
 */
public class AddDefinedColumnRequest implements Request {
    private String tableName;
    private List<DefinedColumnSchema> definedColumns = new ArrayList<DefinedColumnSchema>();

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
     * @param type 预定义列的数据类型。
     */
    public void addDefinedColumn(String name, DefinedColumnType type) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of defined column should not be null or empty.");
        Preconditions.checkNotNull(type, "The type of defined column should not be null.");

        this.definedColumns.add(new DefinedColumnSchema(name, type));
    }

    /**
     * 获取预定义列列表
     * @return 预定义列列表
     */
    public List<DefinedColumnSchema> getDefinedColumn() {
        return Collections.unmodifiableList(definedColumns);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_ADD_DEFINED_COLUMN;
    }
}

