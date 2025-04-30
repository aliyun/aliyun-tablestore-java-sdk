package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.model.search.query.FieldValueFactorFunction;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestFieldValueFactorFunction extends BaseSearchTest {
    @Test
    public void TestSetAndGetFieldName() {
        FieldValueFactorFunction fieldValueFactorFunction = new FieldValueFactorFunction();
        fieldValueFactorFunction.setFieldName("a");
        assertEquals("a", fieldValueFactorFunction.getFieldName());
    }
    @Test
    public void TestSetAndGetFactor() {
        FieldValueFactorFunction fieldValueFactorFunction = new FieldValueFactorFunction();
        fieldValueFactorFunction.setFactor(1f);
        assertEquals(1f, fieldValueFactorFunction.getFactor(), 0.01);
    }

    @Test
    public void TestSetAndGetModifier() {
        FieldValueFactorFunction fieldValueFactorFunction = new FieldValueFactorFunction();
        FieldValueFactorFunction.FunctionModifier modifier = FieldValueFactorFunction.FunctionModifier.LN;
        fieldValueFactorFunction.setModifier(modifier);
        assertEquals(modifier, fieldValueFactorFunction.getModifier());
    }

    @Test
    public void TestSetAndGetMissing() {
        FieldValueFactorFunction fieldValueFactorFunction = new FieldValueFactorFunction();
        fieldValueFactorFunction.setMissing(1d);
        assertEquals(1d, fieldValueFactorFunction.getMissing(), 0.01);
    }

    @Test
    public void TestBuilderFieldName() {
        FieldValueFactorFunction.Builder fieldValueFactorFunction = FieldValueFactorFunction.newBuilder().fieldName("a");
        assertEquals("a", fieldValueFactorFunction.fieldName());
    }

    @Test
    public void TestBuilderFactor() {
        FieldValueFactorFunction.Builder fieldValueFactorFunction = FieldValueFactorFunction.newBuilder().factor(1f);
        assertEquals(1f, fieldValueFactorFunction.factor(), 0.01);
    }

    @Test
    public void TestBuilderModifier() {
        FieldValueFactorFunction.Builder fieldValueFactorFunction = FieldValueFactorFunction.newBuilder().modifier(FieldValueFactorFunction.FunctionModifier.LN);
        assertEquals(FieldValueFactorFunction.FunctionModifier.LN, fieldValueFactorFunction.modifier());
    }

    @Test
    public void TestBuilderMissing() {
        FieldValueFactorFunction.Builder fieldValueFactorFunction = FieldValueFactorFunction.newBuilder().missing(1d);
        assertEquals(1d, fieldValueFactorFunction.missing(), 0.01);
    }
}
