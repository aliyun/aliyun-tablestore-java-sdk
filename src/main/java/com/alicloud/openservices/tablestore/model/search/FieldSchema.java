package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

import java.util.List;

/**
 * SearchIndex的字段的设计
 */
public class FieldSchema implements Jsonizable {

    public enum Analyzer {
        SingleWord("single_word"),
        MaxWord("max_word");

        private String value;
        Analyzer(String value) {
            this.value = value;
        }

        public String toString() {
            return value;
        }

        public static Analyzer fromString(String value) {
            if (value.equals(SingleWord.toString())) {
                return SingleWord;
            } else if (value.equals(MaxWord.toString())) {
                return MaxWord;
            } else {
                throw new ClientException("Unknown analyzer");
            }
        }
    }

    /**
     * 字段名
     */
    private String fieldName;
    /**
     * 字段类型,详见{@link FieldType}
     */
    private FieldType fieldType;
    /**
     * 是否开启索引，默认开启
     */
    private Boolean index = true;
    /**
     *  倒排索引的配置选项
     */
    private IndexOptions indexOptions;
    /**
     * 分词器设置
     */
    private Analyzer analyzer;
    /**
     * 是否开启排序和聚合功能
     */
    private Boolean enableSortAndAgg;

    /**
     * 附加存储，是否在SearchIndex中附加存储该字段的值。
     * 开启后，可以直接从SearchIndex中读取该字段的值，而不必反查主表，可用于查询性能优化。
     */
    private Boolean store;

    /**
     * 存的值是否是一个数组
     */
    private Boolean isArray;

    /**
     * 如果 FiledType 是 NESTED ，则可使用该字段，声明一个嵌套的FieldSchema
     */
    private List<FieldSchema> subFieldSchemas;

    public FieldSchema(String fieldName, FieldType fieldType) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public FieldSchema setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public FieldSchema setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
        return this;
    }

    public Boolean isIndex() {
        return index;
    }

    public FieldSchema setIndex(boolean index) {
        this.index = index;
        return this;
    }

    public IndexOptions getIndexOptions() {
        return indexOptions;
    }

    public FieldSchema setIndexOptions(IndexOptions indexOptions) {
        this.indexOptions = indexOptions;
        return this;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public FieldSchema setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public Boolean isEnableSortAndAgg() {
        return enableSortAndAgg;
    }

    public FieldSchema setEnableSortAndAgg(boolean enableSortAndAgg) {
        this.enableSortAndAgg = enableSortAndAgg;
        return this;
    }

    public Boolean isStore() {
        return store;
    }

    public FieldSchema setStore(Boolean store) {
        this.store = store;
        return this;
    }

    public Boolean isArray() {
        return isArray;
    }

    public FieldSchema setIsArray(boolean array) {
        isArray = array;
        return this;
    }

    public List<FieldSchema> getSubFieldSchemas() {
        return subFieldSchemas;
    }

    public FieldSchema setSubFieldSchemas(List<FieldSchema> subFieldSchemas) {
        this.subFieldSchemas = subFieldSchemas;
        return this;
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
        sb.append("\"FieldName\": \"");
        sb.append(fieldName);
        sb.append("\"");
        sb.append(",");
        sb.append(newline);
        if (fieldType != null) {
            sb.append("\"FieldType\": \"");
            sb.append(fieldType.name());
            sb.append("\"");
            sb.append(",");
            sb.append(newline);
        }
        if (index != null) {
            sb.append("\"Index\": ");
            sb.append(index.toString());
            sb.append(",");
            sb.append(newline);
        }
        if (indexOptions != null) {
            sb.append("\"IndexOptions\": ");
            sb.append(indexOptions.toString());
            sb.append(",");
            sb.append(newline);
        }
        if (analyzer != null) {
            sb.append("\"Analyzer\": ");
            sb.append(analyzer.toString());
            sb.append(",");
            sb.append(newline);
        }
        if (enableSortAndAgg != null) {
            sb.append("\"EnableSortAndAgg\": ");
            sb.append(enableSortAndAgg.toString());
            sb.append(",");
            sb.append(newline);
        }
        if (store != null) {
            sb.append("\"Store\": ");
            sb.append(store.toString());
            sb.append(",");
            sb.append(newline);
        }
        if (isArray != null) {
            sb.append("\"IsArray\": ");
            sb.append(isArray.toString());
            sb.append(",");
            sb.append(newline);
        }
        sb.append("\"SubFieldSchemas\": [");
        boolean first = true;
        for (FieldSchema schema : subFieldSchemas) {
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
