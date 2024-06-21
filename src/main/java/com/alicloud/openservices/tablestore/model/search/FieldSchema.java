package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.model.search.analysis.AnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.FuzzyAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SingleWordAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SplitAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.vector.VectorOptions;

import java.util.Collections;
import java.util.List;

/**
 * SearchIndex的字段的设计
 */
public class FieldSchema implements Jsonizable {

    public enum Analyzer {
        SingleWord("single_word"),
        MaxWord("max_word"),
        MinWord("min_word"),
        Split("split"),
        Fuzzy("fuzzy");

        private final String value;
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
            } else if (value.equals(MinWord.toString())) {
                return MinWord;
            } else if (value.equals(Split.toString())) {
                return Split;
            } else if (value.equals(Fuzzy.toString())) {
                return Fuzzy;
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
     * 字段是否允许摘要和高亮
     */
    private Boolean enableHighlighting;

    /**
     * analyzer
     */
    private Analyzer analyzer;

    /**
     * analyzer parameter
     */
    private AnalyzerParameter analyzerParameter;

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

    /**
     * 是否是虚拟字段
     */
    private Boolean isVirtualField;

    /**
     * 虚拟字段对应的原始字段。
     * 当前仅支持设置一个原始字段。
     */
    private List<String> sourceFieldNames;

    /**
     * 当字段类型是{@link FieldType#DATE}日期类型时候，可以定义该日期支持的格式。
     */
    private List<String> dateFormats;

    /**
     * 当字段类型是{@link FieldType#VECTOR}向量类型时候，可以设置向量配置
     */
    private VectorOptions vectorOptions;

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

    public FieldSchema setEnableHighlighting(boolean enableHighlighting) {
        this.enableHighlighting = enableHighlighting;
        return this;
    }

    public Boolean isEnableHighlighting() {
        return enableHighlighting;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public FieldSchema setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public AnalyzerParameter getAnalyzerParameter() {
        return analyzerParameter;
    }

    public FieldSchema setAnalyzerParameter(AnalyzerParameter analyzerParameter) {
        this.analyzerParameter = analyzerParameter;
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

    public Boolean isVirtualField() {
        return isVirtualField;
    }

    public FieldSchema setVirtualField(Boolean virtualField) {
        isVirtualField = virtualField;
        return this;
    }

    public List<String> getSourceFieldNames() {
        return sourceFieldNames;
    }

    public FieldSchema setSourceFieldName(String sourceFieldName) {
        this.sourceFieldNames = Collections.singletonList(sourceFieldName);
        return this;
    }

    public FieldSchema setSourceFieldNames(List<String> sourceFieldNames) {
        this.sourceFieldNames = sourceFieldNames;
        return this;
    }

    public List<String> getDateFormats() {
        return dateFormats;
    }

    public FieldSchema setDateFormats(List<String> dateFormats) {
        this.dateFormats = dateFormats;
        return this;
    }

    public VectorOptions getVectorOptions() {
        return vectorOptions;
    }

    public FieldSchema setVectorOptions(VectorOptions vectorOptions) {
        this.vectorOptions = vectorOptions;
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
            sb.append("\"Analyzer\": \"");
            sb.append(analyzer.toString());
            sb.append("\",");
            sb.append(newline);

            if (analyzerParameter != null) {
                if (analyzer == Analyzer.SingleWord && analyzerParameter instanceof SingleWordAnalyzerParameter) {
                    sb.append("\"AnalyzerParameter\": {");
                    sb.append("\"CaseSensitive\": ");
                    sb.append(((SingleWordAnalyzerParameter)analyzerParameter).isCaseSensitive());
                    sb.append(", \"DelimitWord\": ");
                    sb.append(((SingleWordAnalyzerParameter)analyzerParameter).isDelimitWord());
                    sb.append("},");
                    sb.append(newline);
                } else if (analyzer == Analyzer.Split && analyzerParameter instanceof SplitAnalyzerParameter) {
                    String delimiter = ((SplitAnalyzerParameter)analyzerParameter).getDelimiter();
                    sb.append("\"AnalyzerParameter\": {");
                    sb.append("\"Delimiter\": ");
                    sb.append(delimiter == null ? "null" : "\"" + delimiter + "\"");
                    sb.append(", \"CaseSensitive\": ");
                    sb.append(((SplitAnalyzerParameter)analyzerParameter).isCaseSensitive());
                    sb.append("},");
                    sb.append(newline);
                } else if (analyzer == Analyzer.Fuzzy && analyzerParameter instanceof FuzzyAnalyzerParameter) {
                    sb.append("\"AnalyzerParameter\": {");
                    sb.append("\"MinChars\": ");
                    sb.append(((FuzzyAnalyzerParameter)analyzerParameter).getMinChars());
                    sb.append(", \"MaxChars\": ");
                    sb.append(((FuzzyAnalyzerParameter)analyzerParameter).getMaxChars());
                    sb.append(", \"CaseSensitive\": ");
                    sb.append(((FuzzyAnalyzerParameter)analyzerParameter).isCaseSensitive());
                    sb.append("},");
                    sb.append(newline);
                }
            }
        }
        if (enableSortAndAgg != null) {
            sb.append("\"EnableSortAndAgg\": ");
            sb.append(enableSortAndAgg.toString());
            sb.append(",");
            sb.append(newline);
        }
        if (enableHighlighting != null) {
            sb.append("\"EnableHighlighting\": ");
            sb.append(enableHighlighting.toString());
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
        if (subFieldSchemas != null) {
            for (int i = 0; i < subFieldSchemas.size(); i++) {
                FieldSchema schema = subFieldSchemas.get(i);
                schema.jsonize(sb, newline);
                if (i != subFieldSchemas.size() - 1) {
                    sb.append(", ");
                    sb.append(newline);
                }
            }
        }
        sb.append("]");
        if (isVirtualField != null) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"IsVirtualField\": ");
            sb.append(isVirtualField);
        }
        if (sourceFieldNames != null) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"SourceFieldNames\": [");
            for (int i = 0; i < sourceFieldNames.size(); i++) {
                String sourceField = sourceFieldNames.get(i);
                sb.append("\"").append(sourceField).append("\"");
                if (i != sourceFieldNames.size() - 1) {
                    sb.append(", ");
                }
            }
            sb.append("]");
        }
        if (dateFormats != null) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"DateFormats\": [");
            for (int i = 0; i < dateFormats.size(); i++) {
                String sourceField = dateFormats.get(i);
                sb.append("\"").append(sourceField).append("\"");
                if (i != dateFormats.size() - 1) {
                    sb.append(", ");
                }
            }
            sb.append("]");
        }
        if (vectorOptions != null) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"VectorOptions\": ");
            sb.append(newline);
            vectorOptions.jsonize(sb, newline);
        }
        sb.append(newline);
        sb.append("}");
    }
}
