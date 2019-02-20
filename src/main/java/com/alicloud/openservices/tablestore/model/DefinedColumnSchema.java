package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class DefinedColumnSchema implements Jsonizable {
    private String name;
    private DefinedColumnType type;

    public DefinedColumnSchema(String name, DefinedColumnType type) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "Defined col's name should not be null or empty.");
        Preconditions.checkNotNull(type, "The type should not be null.");

        this.setName(name);
        this.setType(type);
    }

    /**
     * 获取预定义列的名称。
     * @return 预定义列的名称。
     */
    public String getName() {
        return name;
    }

    /**
     * 设置预定义列的名称。
     * @param name 预定义列的名称。
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 获取预定义列的类型。
     * @return 预定义列的类型。
     */
    public DefinedColumnType getType() {
        return type;
    }

    /**
     * 设置预定义列的类型。
     * @param type 预定义列的类型。
     */
    public void setType(DefinedColumnType type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof DefinedColumnSchema)) {
            return false;
        }

        DefinedColumnSchema target = (DefinedColumnSchema) o;
        return this.name.equals(target.name) && this.type == target.type;
    }

    @Override
    public int hashCode() {
        return this.name.hashCode() ^ this.type.hashCode();
    }

    @Override
    public String toString() {
        return name + ":" + type;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append(String.format(
                "{\"Name\": \"%s\", \"Type\": \"%s\"}",
                name, type.toString()));

    }
}
