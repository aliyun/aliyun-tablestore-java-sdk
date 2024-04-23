package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.model.search.query.DecayFuncDateParam;
import com.alicloud.openservices.tablestore.model.search.query.DecayFuncGeoParam;
import com.alicloud.openservices.tablestore.model.search.query.DecayFuncNumericParam;
import com.alicloud.openservices.tablestore.model.search.query.DecayParam;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDecayParam extends BaseSearchTest {
    @Test
    public void TestUnknownTypeParam() {
        DecayParam param = DecayParam.unknownTypeParam();
        assertEquals(DecayParam.ParamType.UNKNOWN, param.getType());
    }

    @Test
    public void TestDecayFuncDateParamGetAndSetOriginLong() {
        DecayFuncDateParam param = new DecayFuncDateParam();
        param.setOriginLong(20L);
        assertEquals(20L, param.getOriginLong().longValue());
    }

    @Test
    public void TestDecayFuncDateParamGetAndSetOriginString() {
        DecayFuncDateParam param = new DecayFuncDateParam();
        param.setOriginString("2017-01-01");
        assertEquals("2017-01-01", param.getOriginString());
    }

    @Test
    public void TestDecayFuncDateParamGetAndSetScale() {
        DecayFuncDateParam param = new DecayFuncDateParam();
        DateTimeValue dateTimeValue = new DateTimeValue(20, DateTimeUnit.MINUTE);
        param.setScale(dateTimeValue);
        assertEquals(dateTimeValue, param.getScale());
    }

    @Test
    public void TestDecayFuncDateParamGetAndSetOffset() {
        DecayFuncDateParam param = new DecayFuncDateParam();
        DateTimeValue dateTimeValue = new DateTimeValue(20, DateTimeUnit.MINUTE);
        param.setOffset(dateTimeValue);
        assertEquals(dateTimeValue, param.getOffset());
    }

    @Test
    public void TestDecayFuncDateParamBuilderOriginLong() {
        DecayFuncDateParam.Builder builder = DecayFuncDateParam.newBuilder().originLong(20L);
        assertEquals(20L, builder.originLong().longValue());
    }

    @Test
    public void TestDecayFuncDateParamBuilderOriginString() {
        DecayFuncDateParam.Builder builder = DecayFuncDateParam.newBuilder().originString("2017-01-01");
        assertEquals("2017-01-01", builder.originString());
    }

    @Test
    public void TestDecayFuncDateParamBuilderScale() {
        DateTimeValue dateTimeValue = new DateTimeValue(20, DateTimeUnit.MINUTE);
        DecayFuncDateParam.Builder builder = DecayFuncDateParam.newBuilder().scale(dateTimeValue);
        assertEquals(dateTimeValue, builder.scale());
    }

    @Test
    public void TestDecayFuncDateParamBuilderOffset() {
        DateTimeValue dateTimeValue = new DateTimeValue(20, DateTimeUnit.MINUTE);
        DecayFuncDateParam.Builder builder = DecayFuncDateParam.newBuilder().offset(dateTimeValue);
        assertEquals(dateTimeValue, builder.offset());
    }

    @Test
    public void TestDecayFuncGeoParamGetAndSetOrigin() {
        DecayFuncGeoParam param = new DecayFuncGeoParam();
        param.setOrigin("10,10");
        assertEquals("10,10", param.getOrigin());
    }

    @Test
    public void TestDecayFuncGeoParamGetAndSetScale() {
        DecayFuncGeoParam param = new DecayFuncGeoParam();
        param.setScale(10.0);
        assertEquals(10.0, param.getScale(), 0.0001);
    }

    @Test
    public void TestDecayFuncGeoParamGetAndSetOffset() {
        DecayFuncGeoParam param = new DecayFuncGeoParam();
        param.setOffset(10.0);
        assertEquals(10.0, param.getOffset(), 0.0001);
    }

    @Test
    public void TestDecayFuncGeoParamBuilderOrigin() {
        DecayFuncGeoParam.Builder builder = DecayFuncGeoParam.newBuilder().origin("10,10");
        assertEquals("10,10", builder.origin());
    }

    @Test
    public void TestDecayFuncGeoParamBuilderScale() {
        DecayFuncGeoParam.Builder builder = DecayFuncGeoParam.newBuilder().scale(10.0);
        assertEquals(10.0, builder.scale(), 0.0001);
    }

    @Test
    public void TestDecayFuncGeoParamBuilderOffset() {
        DecayFuncGeoParam.Builder builder = DecayFuncGeoParam.newBuilder().offset(10.0);
        assertEquals(10.0, builder.offset(), 0.0001);
    }

    @Test
    public void TestDecayFuncNumericParamGetAndSetOrigin() {
        DecayFuncNumericParam param = new DecayFuncNumericParam();
        param.setOrigin(10.0);
        assertEquals(10.0, param.getOrigin(), 0.0001);
    }

    @Test
    public void TestDecayFuncNumericParamGetAndSetScale() {
        DecayFuncNumericParam param = new DecayFuncNumericParam();
        param.setScale(10.0);
        assertEquals(10.0, param.getScale(), 0.0001);
    }

    @Test
    public void TestDecayFuncNumericParamGetAndSetOffset() {
        DecayFuncNumericParam param = new DecayFuncNumericParam();
        param.setOffset(10.0);
        assertEquals(10.0, param.getOffset(), 0.0001);
    }

    @Test
    public void TestDecayFuncNumericParamBuilderOrigin() {
        DecayFuncNumericParam.Builder builder = DecayFuncNumericParam.newBuilder().origin(10.0);
        assertEquals(10.0, builder.origin(), 0.0001);
    }

    @Test
    public void TestDecayFuncNumericParamBuilderScale() {
        DecayFuncNumericParam.Builder builder = DecayFuncNumericParam.newBuilder().scale(10.0);
        assertEquals(10.0, builder.scale(), 0.0001);
    }

    @Test
    public void TestDecayFuncNumericParamBuilderOffset() {
        DecayFuncNumericParam.Builder builder = DecayFuncNumericParam.newBuilder().offset(10.0);
        assertEquals(10.0, builder.offset(), 0.0001);
    }
}
