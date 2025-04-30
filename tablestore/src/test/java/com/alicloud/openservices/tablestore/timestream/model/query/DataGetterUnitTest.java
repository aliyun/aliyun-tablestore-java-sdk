package com.alicloud.openservices.tablestore.timestream.model.query;

import com.alicloud.openservices.tablestore.ClientException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by yanglian on 2019/8/14.
 */
public class DataGetterUnitTest {
	@Test
	public void testDescTimestamp() {
		DataGetter getter = new DataGetter(null, null, null);
		Assert.assertFalse(getter.isDescTimestamp());

		getter.descTimestamp();
		Assert.assertTrue(getter.isDescTimestamp());
	}

	@Test
	public void testLimit() {
		DataGetter getter = new DataGetter(null, null, null);
		Assert.assertEquals(getter.getLimit(), -1);

		getter.setLimit(123);
		Assert.assertEquals(getter.getLimit(), 123);

		try {
			getter.setLimit(0);
			Assert.assertTrue(false);
		} catch (ClientException e) {
			Assert.assertEquals(e.getMessage(), "The limit must be greater than 0.");
		}

		try {
			getter.setLimit(-1);
			Assert.assertTrue(false);
		} catch (ClientException e) {
			// pass
			Assert.assertEquals(e.getMessage(), "The limit must be greater than 0.");
		}
	}
}
