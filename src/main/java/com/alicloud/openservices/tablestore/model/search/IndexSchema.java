package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;

import java.util.ArrayList;
import java.util.List;

/**
 * index的schema实体类
 */
public class IndexSchema implements Jsonizable {

    /**
     * 关于某个index的设置
     */
    private IndexSetting indexSetting;

    /**
     * 该index的所有字段的设置
     */
    private List<FieldSchema> fieldSchemas;

    /**
     * 自定义索引的预排序方式
     * @return
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
