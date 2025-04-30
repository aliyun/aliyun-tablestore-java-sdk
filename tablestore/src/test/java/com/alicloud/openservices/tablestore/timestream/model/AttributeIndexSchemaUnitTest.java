package com.alicloud.openservices.tablestore.timestream.model;

import junit.framework.Assert;
import org.junit.Test;

/**
 * Created by yanglian on 2019/7/31.
 */
public class AttributeIndexSchemaUnitTest {
	@Test
	public void testSort() {
		AttributeIndexSchema schema = new AttributeIndexSchema("col1", AttributeIndexSchema.Type.KEYWORD);
		Assert.assertTrue(schema.isEnableSortAndAgg() == null);

		schema.setEnableSortAndAgg(true);
		Assert.assertTrue(schema.isEnableSortAndAgg());
	}
}
