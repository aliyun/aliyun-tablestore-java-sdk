package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.model.search.query.DecayFuncDateParam;
import com.alicloud.openservices.tablestore.model.search.query.DecayFuncGeoParam;
import com.alicloud.openservices.tablestore.model.search.query.DecayFuncNumericParam;
import com.alicloud.openservices.tablestore.model.search.query.DecayFunction;
import com.alicloud.openservices.tablestore.model.search.query.DecayParam;
import com.alicloud.openservices.tablestore.model.search.query.MultiValueMode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDecayFunction extends BaseSearchTest {
    @Test
    public void TestSetAndGetFieldName() {
        DecayFunction decayFunction = new DecayFunction();
        decayFunction.setFieldName("field");
        assertEquals("field", decayFunction.getFieldName());
    }

    @Test
    public void TestSetAndGetDecayParam() {
        DecayFunction decayFunction = new DecayFunction();
        DecayFuncGeoParam decayFuncGeoParam = new DecayFuncGeoParam("1,1", 1.0, 1.0);
        decayFunction.setDecayParam(decayFuncGeoParam);
        assertEquals(decayFuncGeoParam, decayFunction.getDecayParam());
    }

    @Test
    public void TestSetAndGetMathFunction() {
        DecayFunction decayFunction = new DecayFunction();
        decayFunction.setMathFunction(DecayFunction.MathFunction.GAUSS);
        assertEquals(DecayFunction.MathFunction.GAUSS, decayFunction.getMathFunction());
    }

    @Test
    public void TestSetAndGetDecay() {
        DecayFunction decayFunction = new DecayFunction();
        decayFunction.setDecay(1.0);
        assertEquals(1.0, decayFunction.getDecay(), 0.00001);
    }

    @Test
    public void TestSetAndGetMultiValueMode() {
        DecayFunction decayFunction = new DecayFunction();
        decayFunction.setMultiValueMode(MultiValueMode.AVG);
        assertEquals(MultiValueMode.AVG, decayFunction.getMultiValueMode());
    }

    @Test
    public void TestBuilderFieldName() {
        DecayFunction.Builder builder = DecayFunction.newBuilder().fieldName("field");
        assertEquals("field", builder.fieldName());
    }

    @Test
    public void TestBuilderDecayParam_Geo() {
        DecayFuncGeoParam decayFuncGeoParam = new DecayFuncGeoParam("1,1", 1.0, 1.0);
        DecayFunction.Builder builder = DecayFunction.newBuilder().decayParam(decayFuncGeoParam);
        assertEquals(decayFuncGeoParam, builder.decayParam());
    }

    @Test
    public void TestDecayGeoParam_GetType() {
        assertEquals(DecayParam.ParamType.GEO, new DecayFuncGeoParam().getType());
    }

    @Test
    public void TestBuilderDecayParam_Date() {
        DecayFuncDateParam decayFuncDateParam = new DecayFuncDateParam(null, "1,1", new DateTimeValue(1, DateTimeUnit.DAY), new DateTimeValue(1, DateTimeUnit.DAY));
        DecayFunction.Builder builder = DecayFunction.newBuilder().decayParam(decayFuncDateParam);
        assertEquals(decayFuncDateParam, builder.decayParam());
    }

    @Test
    public void TestDecayDateParam_GetType() {
        assertEquals(DecayParam.ParamType.DATE, new DecayFuncDateParam().getType());
    }

    @Test
    public void TestBuilderDecayParam_Numeric() {
        DecayFuncNumericParam decayFuncNumericParam = new DecayFuncNumericParam(1.0, 1.0, 1.0);
        DecayFunction.Builder builder = DecayFunction.newBuilder().decayParam(decayFuncNumericParam);
        assertEquals(decayFuncNumericParam, builder.decayParam());
    }

    @Test
    public void TestDecayNumericParam_GetType() {
        assertEquals(DecayParam.ParamType.NUMERIC, new DecayFuncNumericParam().getType());
    }

    @Test
    public void TestBuilderMathFunction() {
        DecayFunction.Builder builder = DecayFunction.newBuilder().mathFunction(DecayFunction.MathFunction.GAUSS);
        assertEquals(DecayFunction.MathFunction.GAUSS, builder.mathFunction());
    }

    @Test
    public void TestBuilderDecay() {
        DecayFunction.Builder builder = DecayFunction.newBuilder().decay(1.0);
        assertEquals(1.0, builder.decay(), 0.00001);
    }

    @Test
    public void TestBuilderMultiValueMode() {
        DecayFunction.Builder builder = DecayFunction.newBuilder().multiValueMode(MultiValueMode.AVG);
        assertEquals(MultiValueMode.AVG, builder.multiValueMode());
    }
}
