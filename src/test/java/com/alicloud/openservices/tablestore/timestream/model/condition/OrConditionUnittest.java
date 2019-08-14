package com.alicloud.openservices.tablestore.timestream.model.condition;

import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import org.junit.Assert;
import org.junit.Test;

import static com.alicloud.openservices.tablestore.timestream.model.condition.ConditionFactory.*;

public class OrConditionUnittest {
    @Test
    public void testBasic() {
        Tag tagFilter = Tag.equal("t1", "v1");
        Attribute attrFilter = Attribute.equal("a1", "v2");
        OrCondition filter = (OrCondition)(or(
                tagFilter,
                attrFilter
        ));
        Query query = filter.getQuery();
        Assert.assertTrue(query instanceof BoolQuery);
        Assert.assertEquals(((BoolQuery)query).getShouldQueries().size(), 2);
        Assert.assertTrue(((BoolQuery)query).getShouldQueries().get(0) instanceof TermQuery);
        Assert.assertEquals(((TermQuery)((BoolQuery)query).getShouldQueries().get(0)).getFieldName(), TableMetaGenerator.CN_PK2);
        Assert.assertEquals(((TermQuery)((BoolQuery)query).getShouldQueries().get(0)).getTerm().asString(), Utils.buildTagValue("t1", "v1"));
        Assert.assertTrue(((BoolQuery)query).getShouldQueries().get(1) instanceof TermQuery);
        Assert.assertEquals(((TermQuery)((BoolQuery)query).getShouldQueries().get(1)).getFieldName(), "a1");
        Assert.assertEquals(((TermQuery)((BoolQuery)query).getShouldQueries().get(1)).getTerm().asString(), "v2");
    }
}
