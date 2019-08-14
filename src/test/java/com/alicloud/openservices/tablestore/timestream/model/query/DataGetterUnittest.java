package com.alicloud.openservices.tablestore.timestream.model.query;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by yanglian on 2019/8/14.
 */
public class DataGetterUnittest {
	@Test
	public void testDescTimestamp() {
		DataGetter getter = new DataGetter(null, null, null);
		Assert.assertFalse(getter.isDescTimestamp());

		getter.descTimestamp();
		Assert.assertTrue(getter.isDescTimestamp());
	}
}
