package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;

import java.util.ArrayList;
import java.util.List;

/**
 * Entity class for the schema of index
 */
public class IndexSchema implements Jsonizable {

    /**
     * Settings related to a specific index.
     */
    private IndexSetting indexSetting;

    /**
     * The setting of all fields for this index
     */
    private List<FieldSchema> fieldSchemas;

    /**
     * Pre-sorting method for custom indexes
     */
    private Sort indexSort;

    public IndexSetting getIndexSetting() {
        return indexSetting;
    }

    public void setIndexSetting(IndexSetting indexSetting) {
        this.indexSetting = indexSetting;
    }

    public List<FieldSchema> getFieldSchemas() {
        return fieldSchemas;
    }

    public void setFieldSchemas(List<FieldSchema> fieldSchemas) {
        this.fieldSchemas = fieldSchemas;
    }

    public void addFieldSchema(FieldSchema fieldSchema) {
        if (this.fieldSchemas == null) {
            this.fieldSchemas = new ArrayList<FieldSchema>();
        }
        this.fieldSchemas.add(fieldSchema);
    }

    public Sort getIndexSort() {
        return indexSort;
    }

    public void setIndexSort(Sort indexSort) {
        this.indexSort = indexSort;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        sb.append(newline);
        sb.append("\"IndexSetting\": ");
        indexSetting.jsonize(sb, newline + "  ");
        sb.append(",");
        sb.append(newline);
        sb.append("\"FieldSchemas\": [");
        boolean first = true;
        for (FieldSchema schema : fieldSchemas) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
                sb.append(newline + " ");
            }
            schema.jsonize(sb, newline + " ");
        }
        sb.append("]");
        sb.append(newline.substring(0, newline.length() - 2));
        sb.append("}");
    }
}
