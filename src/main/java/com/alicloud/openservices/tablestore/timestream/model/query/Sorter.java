package com.alicloud.openservices.tablestore.timestream.model.query;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.GeoDistanceSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yanglian on 2019/7/22.
 */
public class Sorter {
	public enum SortOrder {
		ASC,
		DESC;

		public com.alicloud.openservices.tablestore.model.search.sort.SortOrder toSearchSortOrder() {
			if (this == ASC) {
				return com.alicloud.openservices.tablestore.model.search.sort.SortOrder.ASC;
			} else {
				return com.alicloud.openservices.tablestore.model.search.sort.SortOrder.DESC;
			}
		}
	}

	public final static class Builder {
		private Sorter sorter = null;

		private Builder() {}

		public static Builder newBuilder() {
			return new Builder();
		}

		/**
		 * 按照name进行排序
		 * @param sortOrder
		 * @return
		 */
		public Builder sortName(SortOrder sortOrder) {
			return this.sortAttributes(TableMetaGenerator.CN_PK1, sortOrder);
		}

		/**
		 * 对属性列进行排序
		 * @param key
		 * @param sortOrder
		 * @return
		 */
		public Builder sortAttributes(String key, SortOrder sortOrder) {
			if (this.sorter == null) {
				this.sorter = new Sorter();
			}
			this.sorter.addSubSorter(
					new FieldSort(
							key, sortOrder.toSearchSortOrder()
					)
			);
			return this;
		}

		/**
		 * 对属性列根据地理位置进行排序
		 * @param key
		 * @param center
		 * @param sortOrder
		 * @return
		 */
		public Builder sortAttributesInGeo(String key, String center, SortOrder sortOrder) {
			if (this.sorter == null) {
				this.sorter = new Sorter();
			}
			GeoDistanceSort tmpSort = new GeoDistanceSort(key, Arrays.asList(center));
			tmpSort.setOrder(sortOrder.toSearchSortOrder());
			this.sorter.addSubSorter(tmpSort);
			return this;
		}

		public Sorter build() {
			if (sorter == null) {
				throw new ClientException("");
			}
			return this.sorter;
		}
	}

	private List<Sort.Sorter> sorterList;

	private Sorter() {
		this.sorterList = new ArrayList<Sort.Sorter>();
	}

	private Sorter addSubSorter(Sort.Sorter sorter) {
		this.sorterList.add(sorter);
		return this;
	}

	public List<Sort.Sorter> getSorter() {
		return sorterList;
	}
}
