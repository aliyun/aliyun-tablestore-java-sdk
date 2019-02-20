package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class PrimaryKeySchema implements Jsonizable {
    private String name;
    private PrimaryKeyType type;
    private PrimaryKeyOption option;

    public PrimaryKeySchema(String name, PrimaryKeyType type) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "Primary key's name should not be null or empty.");
        Preconditions.checkNotNull(type, "The type should not be null");
        this.setName(name);
        this.setType(type);
        this.setOption(null);
    }
    
    public PrimaryKeySchema(String name, PrimaryKeyType type, PrimaryKeyOption option) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "Primary key's name should not be null or empty.");
        Preconditions.checkNotNull(type, "The type should not be null.");
        Preconditions.checkNotNull(option, "The option should not be null.");
        Preconditions.checkArgument((option != PrimaryKeyOption.AUTO_INCREMENT) || type == PrimaryKeyType.INTEGER,
                "Auto_Increment PK must be Integer type.");

        this.setName(name);
        this.setType(type);
        this.setOption(option);
    }
    
    /**
     * 获取主键的名称。
     * @return 主键的名称。
     */
    public String getName() {
        return name;
    }

    /**
     * 设置主键的名称。
     * @param name 主键的名称。
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 获取主键的类型。
     * @return 主键的类型。
     */
    public PrimaryKeyType getType() {
        return type;
    }

    /**
     * 设置主键的类型。
     * @param type 主键的类型。
     */
    public void setType(PrimaryKeyType type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof PrimaryKeySchema)) {
            return false;
        }

        PrimaryKeySchema target = (PrimaryKeySchema) o;
        return this.name.equals(target.name) && this.type == target.type && this.option == target.option;
    }

    @Override
    public int hashCode() {
        return this.name.hashCode() ^ this.type.hashCode() ^ (hasOption() ? this.option.hashCode() : 0);
    }

    @Override
    public String toString() {
        return name + ":" + type + (hasOption() ? (":" + option) : "");
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        if (hasOption()) {
            sb.append(String.format(
                    "{\"Name\": \"%s\", \"Type\": \"%s\", \"Option\":\"%s\"}",
                    name, type.toString(), option.toString()));
        } else {
            sb.append(String.format(
                    "{\"Name\": \"%s\", \"Type\": \"%s\"}",
                    name, type.toString()));
        }
    }

    public boolean hasOption() {
        return option != null;
    }

    public PrimaryKeyOption getOption() {
        return option;
    }

    public void setOption(PrimaryKeyOption option) {
        this.option = option;
    }
}
