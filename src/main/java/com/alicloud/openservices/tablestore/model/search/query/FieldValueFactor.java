package com.alicloud.openservices.tablestore.model.search.query;

/**
 * @Deprecated 原本用于 {@link FunctionScoreQuery}的此类已经被废弃，请使用{@link FunctionsScoreQuery}代替原有功能。
 * <p>
 * field_value_factor的目的是通过文档中某个字段的值计算出一个分数,以此分数来影响文档的排序。请结合{@link FunctionScoreQuery} 使用。
 * <p>举例：HR管理系统的场景，我们想查名字中包含“王”、出生地包含“京”的人，但是想让结果根据根据身高排序。就可以把身高设置在FieldValueFactor中</p>
 */
@Deprecated
public class FieldValueFactor {

    private String fieldName;

    public FieldValueFactor(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
}
