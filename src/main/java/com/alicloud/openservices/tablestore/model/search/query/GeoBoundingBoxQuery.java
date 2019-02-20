package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 找出经纬度落在指定矩形内的数据。
 * <p>场景举例：订单区域画像分析的场景，想分析A小区购买力，而恰好这A小区是矩形的。我们通过统计A小区订单数量（或总价）即可。</p>
 * <p>方法：在SearchQuery的中构造一个{@link BoolQuery},其 mustQueries 中放入一个{@link GeoBoundingBoxQuery}的矩形地理位置，然后mustQueries再放入查询订单数量的query，就可以获得想要的结果。</p>
 */
public class GeoBoundingBoxQuery implements Query {

    private String fieldName;
    /**
     * 矩形的左上角的经纬度
     * <p>示例："46.24123424, 23.2342424"</p>
     */
    private String topLeft;
    /**
     * 矩形的右下角经纬度
     * <p>示例："46.24123424, 23.2342424"</p>
     */
    private String bottomRight;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getTopLeft() {
        return topLeft;
    }

    public void setTopLeft(String topLeft) {
        this.topLeft = topLeft;
    }

    public String getBottomRight() {
        return bottomRight;
    }

    public void setBottomRight(String bottomRight) {
        this.bottomRight = bottomRight;
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.QueryType_GeoBoundingBoxQuery;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildGeoBoundingBoxQuery(this).toByteString();
    }
}
