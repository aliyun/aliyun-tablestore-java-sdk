package com.alicloud.openservices.tablestore.model.search;

/**
 * 字段折叠，能够实现某个字段的结果去重
 * <p>场景举例：</p>
 * <p>app点餐场景下，我想吃八大菜系最火的菜。如果用传统的方法，我们可能需要对8个菜的type进行分别查询最火的菜。
 * 但是我们通过设置{@link Collapse}为菜系type，就可以返回8个最火的菜（每个菜系只返回一个，因为{@link Collapse}帮我们进行了去重）。一次查询搞定用户的需求。</p>
 */
public class Collapse {
    public Collapse(String fieldName) {
        this.fieldName = fieldName;
    }

    private String fieldName;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
}
