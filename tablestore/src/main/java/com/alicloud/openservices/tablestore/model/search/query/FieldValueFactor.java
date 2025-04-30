package com.alicloud.openservices.tablestore.model.search.query;

/**
 * @Deprecated The class originally used for {@link FunctionScoreQuery} has been deprecated. Please use {@link FunctionsScoreQuery} to replace the original functionality.
 * <p>
 * The purpose of field_value_factor is to calculate a score based on the value of a certain field in the document, and use this score to influence the document ranking. It should be used in conjunction with {@link FunctionScoreQuery}.
 * <p>Example: In an HR management system scenario, if we want to search for people whose names contain "Wang" and whose birthplace contains "Jing", but we want the results to be sorted by height, we can set the height in FieldValueFactor.</p>
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
