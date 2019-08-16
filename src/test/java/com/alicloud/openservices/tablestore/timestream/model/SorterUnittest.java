package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.GeoDistanceSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.model.query.Sorter;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Created by yanglian on 2019/7/22.
 */
public class SorterUnittest {
	@Test
	public void testBasic() {
		Sorter sorter = Sorter.Builder.newBuilder().sortByName(Sorter.SortOrder.DESC).build();
		{
			List<Sort.Sorter> sorterList = sorter.getSorter();
			Assert.assertEquals(sorterList.size(), 1);
			Assert.assertTrue(sorterList.get(0) instanceof FieldSort);
			FieldSort nameSort = (FieldSort) sorterList.get(0);
			Assert.assertEquals(nameSort.getFieldName(), TableMetaGenerator.CN_PK1);
			Assert.assertEquals(nameSort.getOrder(), SortOrder.DESC);
		}

		sorter = Sorter.Builder.newBuilder()
				.sortByAttributes("col1", Sorter.SortOrder.DESC)
				.sortByAttributesInGeo("col2", "0, 1", Sorter.SortOrder.ASC)
				.sortByName(Sorter.SortOrder.DESC).build();
		{
			List<Sort.Sorter> sorterList = sorter.getSorter();
			Assert.assertEquals(sorterList.size(), 3);

			Assert.assertTrue(sorterList.get(0) instanceof FieldSort);
			FieldSort fieldSort = (FieldSort) sorterList.get(0);
			Assert.assertEquals(fieldSort.getFieldName(), "col1");
			Assert.assertEquals(fieldSort.getOrder(), SortOrder.DESC);

			Assert.assertTrue(sorterList.get(1) instanceof GeoDistanceSort);
			GeoDistanceSort geoSort = (GeoDistanceSort) sorterList.get(1);
			Assert.assertEquals(geoSort.getFieldName(), "col2");
			Assert.assertEquals(geoSort.getPoints().size(), 1);
			Assert.assertEquals(geoSort.getPoints().get(0), "0, 1");
			Assert.assertEquals(geoSort.getOrder(), SortOrder.ASC);

			Assert.assertTrue(sorterList.get(2) instanceof FieldSort);
			FieldSort nameSort = (FieldSort) sorterList.get(2);
			Assert.assertEquals(nameSort.getFieldName(), TableMetaGenerator.CN_PK1);
			Assert.assertEquals(nameSort.getOrder(), SortOrder.DESC);
		}
	}
}
