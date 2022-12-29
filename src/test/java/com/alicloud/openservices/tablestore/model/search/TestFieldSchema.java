package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.model.search.analysis.AnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.FuzzyAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SingleWordAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SplitAnalyzerParameter;
import com.google.common.base.Supplier;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.*;

import com.google.gson.Gson;

public class TestFieldSchema extends BaseSearchTest {
    @Test
    public void jsonizeSingleWordNoParam() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);

        // sub field schemas
        List<FieldSchema> subSchemas = new ArrayList<FieldSchema>();
        FieldSchema subFieldSchema = new FieldSchema("Col_Sub", FieldType.TEXT);
        subFieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());
        subSchemas.add(subFieldSchema);
        fieldSchema.setSubFieldSchemas(subSchemas);

        // analyzers
        FieldSchema.Analyzer analyzerSingle = FieldSchema.Analyzer.SingleWord;
        fieldSchema.setAnalyzer(analyzerSingle);

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertEquals("single_word", fieldSchemaMap.get("Analyzer"));
        assertNull(fieldSchemaMap.get("AnalyzerParameter"));
    }

    @Test
    public void jsonizeSingleWordWithParam() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);

        // sub field schemas
        List<FieldSchema> subSchemas = new ArrayList<FieldSchema>();
        FieldSchema subFieldSchema = new FieldSchema("Col_Sub", FieldType.TEXT);
        subFieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());
        subSchemas.add(subFieldSchema);
        fieldSchema.setSubFieldSchemas(subSchemas);

        // analyzer
        FieldSchema.Analyzer analyzerSingle = FieldSchema.Analyzer.SingleWord;
        fieldSchema.setAnalyzer(analyzerSingle);

        AnalyzerParameter param = new SingleWordAnalyzerParameter(Boolean.TRUE, Boolean.TRUE);
        fieldSchema.setAnalyzerParameter(param);

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertEquals("single_word", fieldSchemaMap.get("Analyzer"));
        @SuppressWarnings("unchecked")
        Map<String, Object> analyzerParamMap = (Map<String, Object>)fieldSchemaMap.get("AnalyzerParameter");
        assertTrue((Boolean) analyzerParamMap.get("CaseSensitive"));
        assertTrue((Boolean) analyzerParamMap.get("DelimitWord"));
    }

    @Test
    public void jsonizeSingleWordWithDefaultParam() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);

        // sub field schemas
        List<FieldSchema> subSchemas = new ArrayList<FieldSchema>();
        FieldSchema subFieldSchema = new FieldSchema("Col_Sub", FieldType.TEXT);
        subFieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());
        subSchemas.add(subFieldSchema);
        fieldSchema.setSubFieldSchemas(subSchemas);

        // analyzer
        FieldSchema.Analyzer analyzerSingle = FieldSchema.Analyzer.SingleWord;
        fieldSchema.setAnalyzer(analyzerSingle);

        AnalyzerParameter param = new SingleWordAnalyzerParameter();
        fieldSchema.setAnalyzerParameter(param);

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertEquals("single_word", fieldSchemaMap.get("Analyzer"));
        @SuppressWarnings("unchecked")
        Map<String, Object> analyzerParamMap = (Map<String, Object>)fieldSchemaMap.get("AnalyzerParameter");
        assertNull(analyzerParamMap.get("CaseSensitive"));
        assertNull(analyzerParamMap.get("DelimitWord"));
    }

    @Test
    public void jsonizeSplitNoParam() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);
        fieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());

        // analyzers
        FieldSchema.Analyzer analyzerSplit = FieldSchema.Analyzer.Split;
        fieldSchema.setAnalyzer(analyzerSplit);

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertEquals("split", fieldSchemaMap.get("Analyzer"));
        assertNull(fieldSchemaMap.get("AnalyzerParameter"));
    }

    @Test
    public void jsonizeSplitWithParam() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);
        fieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());

        // analyzers
        FieldSchema.Analyzer analyzerSplit = FieldSchema.Analyzer.Split;
        fieldSchema.setAnalyzer(analyzerSplit);

        AnalyzerParameter param = new SplitAnalyzerParameter("-");
        fieldSchema.setAnalyzerParameter(param);

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertEquals("split", fieldSchemaMap.get("Analyzer"));
        @SuppressWarnings("unchecked")
        Map<String, Object> analyzerParamMap = (Map<String, Object>)fieldSchemaMap.get("AnalyzerParameter");
        assertEquals("-", analyzerParamMap.get("Delimiter"));
    }

    @Test
    public void jsonizeSplitWithDefaultParam() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);
        fieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());

        // analyzers
        FieldSchema.Analyzer analyzerSplit = FieldSchema.Analyzer.Split;
        fieldSchema.setAnalyzer(analyzerSplit);

        AnalyzerParameter param = new SplitAnalyzerParameter();
        fieldSchema.setAnalyzerParameter(param);

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertEquals("split", fieldSchemaMap.get("Analyzer"));
        @SuppressWarnings("unchecked")
        Map<String, Object> analyzerParamMap = (Map<String, Object>)fieldSchemaMap.get("AnalyzerParameter");
        assertNull(analyzerParamMap.get("Delimiter"));
    }

    @Test
    public void jsonizeFuzzyNoParam() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);
        fieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());

        // analyzers
        FieldSchema.Analyzer analyzerFuzzy = FieldSchema.Analyzer.Fuzzy;
        fieldSchema.setAnalyzer(analyzerFuzzy);

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertEquals("fuzzy", fieldSchemaMap.get("Analyzer"));
        assertNull(fieldSchemaMap.get("AnalyzerParameter"));
    }

    @Test
    public void jsonizeFuzzyWithParam() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);
        fieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());

        // analyzers
        FieldSchema.Analyzer analyzerFuzzy = FieldSchema.Analyzer.Fuzzy;
        fieldSchema.setAnalyzer(analyzerFuzzy);

        AnalyzerParameter param = new FuzzyAnalyzerParameter(1, 3);
        fieldSchema.setAnalyzerParameter(param);

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertEquals("fuzzy", fieldSchemaMap.get("Analyzer"));
        @SuppressWarnings("unchecked")
        Map<String, Object> analyzerParamMap = (Map<String, Object>)fieldSchemaMap.get("AnalyzerParameter");
        assertEquals(1, ((Double)analyzerParamMap.get("MinChars")).intValue());
        assertEquals(3, ((Double)analyzerParamMap.get("MaxChars")).intValue());
    }

    @Test
    public void jsonizeFuzzyWithDefaultParam() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);
        fieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());

        // analyzers
        FieldSchema.Analyzer analyzerFuzzy = FieldSchema.Analyzer.Fuzzy;
        fieldSchema.setAnalyzer(analyzerFuzzy);

        AnalyzerParameter param = new FuzzyAnalyzerParameter();
        fieldSchema.setAnalyzerParameter(param);

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertEquals("fuzzy", fieldSchemaMap.get("Analyzer"));
        @SuppressWarnings("unchecked")
        Map<String, Object> analyzerParamMap = (Map<String, Object>)fieldSchemaMap.get("AnalyzerParameter");
        assertNull(analyzerParamMap.get("MinChars"));
        assertNull(analyzerParamMap.get("MaxChars"));
    }

    @Test
    public void jsonizeMinWordNoParam() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);
        fieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());

        // analyzers
        FieldSchema.Analyzer analyzerMinWord = FieldSchema.Analyzer.MinWord;
        fieldSchema.setAnalyzer(analyzerMinWord);

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertEquals("min_word", fieldSchemaMap.get("Analyzer"));
        assertNull(fieldSchemaMap.get("AnalyzerParameter"));
    }

    @Test
    public void jsonizeMinWordInvalid() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);
        fieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());

        // analyzers
        FieldSchema.Analyzer analyzerMinWord = FieldSchema.Analyzer.MinWord;
        fieldSchema.setAnalyzer(analyzerMinWord);

        // analyzer param ignored
        AnalyzerParameter param = new FuzzyAnalyzerParameter(1, 3);
        fieldSchema.setAnalyzerParameter(param);

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertEquals("min_word", fieldSchemaMap.get("Analyzer"));
        assertNull(fieldSchemaMap.get("AnalyzerParameter"));
    }

    @Test
    public void jsonizeMaxWordNoParam() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);
        fieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());

        // analyzers
        FieldSchema.Analyzer analyzerMaxWord = FieldSchema.Analyzer.MaxWord;
        fieldSchema.setAnalyzer(analyzerMaxWord);

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertEquals("max_word", fieldSchemaMap.get("Analyzer"));
        assertNull(fieldSchemaMap.get("AnalyzerParameter"));
    }

    @Test
    public void jsonizeMaxWordInvalid() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);
        fieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());

        // analyzers
        FieldSchema.Analyzer analyzerMaxWord = FieldSchema.Analyzer.MaxWord;
        fieldSchema.setAnalyzer(analyzerMaxWord);

        // analyzer param ignored
        AnalyzerParameter param = new FuzzyAnalyzerParameter(1, 3);
        fieldSchema.setAnalyzerParameter(param);

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertEquals("max_word", fieldSchemaMap.get("Analyzer"));
        assertNull(fieldSchemaMap.get("AnalyzerParameter"));
    }

    @Test
    public void jsonizeAnalyzerParamWithNoAnalyzer() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);
        fieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());

        AnalyzerParameter param = new FuzzyAnalyzerParameter(1, 3);
        fieldSchema.setAnalyzerParameter(param);

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertNull(fieldSchemaMap.get("Analyzer"));
        assertNull(fieldSchemaMap.get("AnalyzerParameter"));
    }

    @Test
    public void jsonizeNoAnalyzerNoAnalyzerParam() {
        FieldSchema fieldSchema = new FieldSchema("Col_Analyzer", FieldType.TEXT);
        fieldSchema.setSubFieldSchemas(new ArrayList<FieldSchema>());

        Map<?, ?> fieldSchemaMap = new Gson().fromJson(fieldSchema.jsonize(), Map.class);
        assertNull(fieldSchemaMap.get("Analyzer"));
        assertNull(fieldSchemaMap.get("AnalyzerParameter"));
    }

    @Test
    public void testVirtualField() {
        Boolean isVirtualField = random().nextBoolean() ? null : random().nextBoolean();
        String sourceFieldName = randomString(random().nextInt(5) + 1);
        FieldType fieldType = randomFrom(FieldType.values());

        FieldSchema fieldSchema = new FieldSchema("f1", fieldType);
        fieldSchema.setVirtualField(isVirtualField);
        fieldSchema.setSourceFieldName(sourceFieldName);

        assertEquals(sourceFieldName, fieldSchema.getSourceFieldNames().get(0));
        assertEquals(fieldType, fieldSchema.getFieldType());
        assertEquals(isVirtualField, fieldSchema.isVirtualField());
    }

    @Test
    public void testSourceFieldName() {
        Boolean isVirtualField = random().nextBoolean() ? null : random().nextBoolean();

        List<String> sourceFieldNames = random().nextBoolean() ? Collections.singletonList(randomString(random().nextInt(5) + 1)) :
                randomList(new Supplier<String>() {
                    @Override
                    public String get() {
                        return randomString(random().nextInt(5));
                    }
                });
        FieldType fieldType = randomFrom(FieldType.values());

        FieldSchema fieldSchema = new FieldSchema("f1", fieldType);
        fieldSchema.setVirtualField(isVirtualField);
        fieldSchema.setSourceFieldNames(sourceFieldNames);

        assertEquals(sourceFieldNames, fieldSchema.getSourceFieldNames());
        assertEquals(fieldType, fieldSchema.getFieldType());
        assertEquals(isVirtualField, fieldSchema.isVirtualField());
    }

    @Test
    public void testVirtualFieldJsonize() {
        Boolean isVirtualField = random().nextBoolean() ? null : random().nextBoolean();
        List<String> sourceFieldNames = random().nextBoolean() ?
                (random().nextBoolean() ? null : Collections.singletonList(randomString(random().nextInt(5) + 1))) :
                randomList(new Supplier<String>() {
                    @Override
                    public String get() {
                        return randomString(random().nextInt(5) + 1);
                    }
                });
        FieldType fieldType = randomFrom(FieldType.values());

        FieldSchema fieldSchema = new FieldSchema("f1", fieldType);
        fieldSchema.setVirtualField(isVirtualField);
        fieldSchema.setSourceFieldNames(sourceFieldNames);
        String jsonString = fieldSchema.jsonize();
        Map<?, ?> fieldSchemaMap = new Gson().fromJson(jsonString, Map.class);
        assertEquals("f1", fieldSchemaMap.get("FieldName"));
        assertEquals(isVirtualField, fieldSchemaMap.get("IsVirtualField"));
        assertEquals(sourceFieldNames, fieldSchemaMap.get("SourceFieldNames"));
    }

    @Test
    public void testDateFormats() {
        List<String> formats =
                random().nextBoolean() ? Collections.singletonList(randomString(random().nextInt(5) + 1)) :
                        randomList(new Supplier<String>() {
                            @Override
                            public String get() {
                                return randomString(random().nextInt(5) + 1);
                            }
                        });
        FieldType fieldType = randomFrom(FieldType.values());
        FieldSchema fieldSchema = new FieldSchema("f1", fieldType);
        assertNull(fieldSchema.getDateFormats());
        fieldSchema.setDateFormats(formats);
        assertArrayEquals(fieldSchema.getDateFormats().toArray(new String[0]), formats.toArray(new String[0]));
    }

    @Test
    public void testDateFormatsJsonize() {
        List<String> formats = random().nextBoolean() ?
                (random().nextBoolean() ? null : Collections.singletonList(randomString(random().nextInt(5) + 1))) :
                randomList(new Supplier<String>() {
                    @Override
                    public String get() {
                        return randomString(random().nextInt(5) + 1);
                    }
                });
        FieldType fieldType = randomFrom(FieldType.values());

        FieldSchema fieldSchema = new FieldSchema("f1", fieldType);
        fieldSchema.setDateFormats(formats);
        String jsonString = fieldSchema.jsonize();
        Map<?, ?> fieldSchemaMap = new Gson().fromJson(jsonString, Map.class);
        assertEquals("f1", fieldSchemaMap.get("FieldName"));
        assertEquals(formats, fieldSchemaMap.get("DateFormats"));
    }
}
