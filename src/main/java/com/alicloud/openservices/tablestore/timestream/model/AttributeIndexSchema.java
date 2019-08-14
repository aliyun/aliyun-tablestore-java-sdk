package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;

/**
 * Created by yanglian on 2019/4/9.
 */
public class AttributeIndexSchema {
    public enum Type {
        LONG,
        DOUBLE,
        BOOLEAN,
        /**
         * 字符串类型
         */
        KEYWORD,
        GEO_POINT
    }
    /**
     * 字段类型,详见{@link Type}
     */
    private Type type;

    private FieldSchema fieldSchema;

    public AttributeIndexSchema(String fieldName, Type type) {
        this.fieldSchema = new FieldSchema(fieldName, convertType(type));
        this.type = type;
    }

    private static FieldType convertType(Type type) {
        if (type == Type.LONG) {
            return FieldType.LONG;
        } else if (type == Type.DOUBLE) {
            return FieldType.DOUBLE;
        } else if (type == Type.BOOLEAN) {
            return FieldType.BOOLEAN;
        } else if (type == Type.KEYWORD) {
            return FieldType.KEYWORD;
        } else if (type == Type.GEO_POINT) {
            return FieldType.GEO_POINT;
        } else {
            throw new ClientException("Unsupported type: " + type + ".");
        }
    }

    public String getFieldName() {
        return this.fieldSchema.getFieldName();
    }

    public Type getFieldType() {
        return type;
    }

    public AttributeIndexSchema setIndex(boolean index) {
        this.fieldSchema.setIndex(index);
        return this;
    }

    public Boolean isIndex() {
        return this.fieldSchema.isIndex();
    }

    public AttributeIndexSchema setStore(Boolean store) {
        this.fieldSchema.setStore(store);
        return this;
    }

    public Boolean isStore() {
        return this.fieldSchema.isStore();
    }

    public AttributeIndexSchema setIsArray(boolean array) {
        this.fieldSchema.setIsArray(array);
        return this;
    }

    public Boolean isArray() {
        return this.fieldSchema.isArray();
    }

    public AttributeIndexSchema setEnableSortAndAgg(boolean enableSortAndAgg) {
        this.fieldSchema.setEnableSortAndAgg(enableSortAndAgg);
        return this;
    }

    public Boolean isEnableSortAndAgg() {
        return this.fieldSchema.isEnableSortAndAgg();
    }

    public FieldSchema getFieldSchema() {
        return this.fieldSchema;
    }
}
