package com.alicloud.openservices.tablestore.model.search.analysis;

import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import org.junit.Test;
import static org.junit.Assert.*;

public class AnalyzerParameterBuilderTest extends BaseSearchTest {

    @Test
    public void buildSingleWordAnalyzerParameterWithDefaultParameter() {
        SingleWordAnalyzerParameter param = new SingleWordAnalyzerParameter();
        Search.SingleWordAnalyzerParameter pbParam = AnalyzerParameterBuilder.buildSingleWordAnalyzerParameter(param);
        assertFalse(pbParam.getCaseSensitive());
        assertFalse(pbParam.getDelimitWord());
    }

    @Test
    public void buildSingleWordAnalyzerParameterWithParameter() {
        SingleWordAnalyzerParameter param = new SingleWordAnalyzerParameter(true, true);
        Search.SingleWordAnalyzerParameter pbParam = AnalyzerParameterBuilder.buildSingleWordAnalyzerParameter(param);
        assertTrue(pbParam.getCaseSensitive());
        assertTrue(pbParam.getDelimitWord());
    }

    @Test
    public void buildSplitAnalyzerParameterWithDefaultParameter() {
        SplitAnalyzerParameter param = new SplitAnalyzerParameter();
        Search.SplitAnalyzerParameter pbParam = AnalyzerParameterBuilder.buildSplitAnalyzerParameter(param);
        assertFalse(pbParam.hasDelimiter());
    }

    @Test
    public void buildSplitAnalyzerParameterWithParameter() {
        SplitAnalyzerParameter param = new SplitAnalyzerParameter("-");
        Search.SplitAnalyzerParameter pbParam = AnalyzerParameterBuilder.buildSplitAnalyzerParameter(param);
        assertEquals("-", pbParam.getDelimiter());
    }

    @Test
    public void buildFuzzyAnalyzerParameterWithDefaultParameter() {
        FuzzyAnalyzerParameter param = new FuzzyAnalyzerParameter();
        Search.FuzzyAnalyzerParameter pbParam = AnalyzerParameterBuilder.buildFuzzyAnalyzerParameter(param);
        assertFalse(pbParam.hasMinChars());
        assertFalse(pbParam.hasMaxChars());
    }

    @Test
    public void buildFuzzyAnalyzerParameterWithParameter() {
        FuzzyAnalyzerParameter param = new FuzzyAnalyzerParameter(2, 4);
        Search.FuzzyAnalyzerParameter pbParam = AnalyzerParameterBuilder.buildFuzzyAnalyzerParameter(param);
        assertEquals(2, pbParam.getMinChars());
        assertEquals(4, pbParam.getMaxChars());
    }
}
