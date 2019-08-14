package com.alicloud.openservices.tablestore.timestream.model.condition;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.timestream.model.expression.*;

import java.util.List;

/**
 * {@link com.alicloud.openservices.tablestore.timestream.model.TimestreamMeta#attributes}过滤器，查询满足条件的TimestreamMeta
 */
public class Attribute implements Condition {
    private String key;
    private Expression expression;

    private Attribute() {}

    public static Attribute equal(String key, ColumnValue value) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new EqualExpression(value);
        return condition;
    }

    public static Attribute equal(String key, String value) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new EqualExpression(ColumnValue.fromString(value));
        return condition;
    }

    public static Attribute equal(String key, long value) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new EqualExpression(ColumnValue.fromLong(value));
        return condition;
    }

    public static Attribute equal(String key, byte[] value) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new EqualExpression(ColumnValue.fromBinary(value));
        return condition;
    }

    public static Attribute equal(String key, double value) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new EqualExpression(ColumnValue.fromDouble(value));
        return condition;
    }

    public static Attribute equal(String key, boolean value) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new EqualExpression(ColumnValue.fromBoolean(value));
        return condition;
    }

    public static Attribute notEqual(String key, ColumnValue value) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new NotEqualExpression(value);
        return condition;
    }

    public static Attribute notEqual(String key, String value) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new NotEqualExpression(ColumnValue.fromString(value));
        return condition;
    }

    public static Attribute notEqual(String key, long value) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new NotEqualExpression(ColumnValue.fromLong(value));
        return condition;
    }

    public static Attribute notEqual(String key, byte[] value) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new NotEqualExpression(ColumnValue.fromBinary(value));
        return condition;
    }

    public static Attribute notEqual(String key, double value) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new NotEqualExpression(ColumnValue.fromDouble(value));
        return condition;
    }

    public static Attribute notEqual(String key, boolean value) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new NotEqualExpression(ColumnValue.fromBoolean(value));
        return condition;
    }

    public static Attribute in(String key, ColumnValue[] valueList) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new InExpression(valueList);
        return condition;
    }

    public static Attribute in(String key, String[] valueList) {
        Attribute condition = new Attribute();
        condition.key = key;
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromString(valueList[i]);
        }
        condition.expression = new InExpression(columnValues);
        return condition;
    }

    public static Attribute in(String key, long[] valueList) {
        Attribute condition = new Attribute();
        condition.key = key;
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromLong(valueList[i]);
        }
        condition.expression = new InExpression(columnValues);
        return condition;
    }

    public static Attribute in(String key, byte[][] valueList) {
        Attribute condition = new Attribute();
        condition.key = key;
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromBinary(valueList[i]);
        }
        condition.expression = new InExpression(columnValues);
        return condition;
    }

    public static Attribute in(String key, double[] valueList) {
        Attribute condition = new Attribute();
        condition.key = key;
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromDouble(valueList[i]);
        }
        condition.expression = new InExpression(columnValues);
        return condition;
    }

    public static Attribute in(String key, boolean[] valueList) {
        Attribute condition = new Attribute();
        condition.key = key;
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromBoolean(valueList[i]);
        }
        condition.expression = new InExpression(columnValues);
        return condition;
    }

    public static Attribute notIn(String key, ColumnValue[] valueList) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new NotInExpression(valueList);
        return condition;
    }

    public static Attribute notIn(String key, String[] valueList) {
        Attribute condition = new Attribute();
        condition.key = key;
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromString(valueList[i]);
        }
        condition.expression = new NotInExpression(columnValues);
        return condition;
    }

    public static Attribute notIn(String key, long[] valueList) {
        Attribute condition = new Attribute();
        condition.key = key;
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromLong(valueList[i]);
        }
        condition.expression = new NotInExpression(columnValues);
        return condition;
    }

    public static Attribute notIn(String key, byte[][] valueList) {
        Attribute condition = new Attribute();
        condition.key = key;
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromBinary(valueList[i]);
        }
        condition.expression = new NotInExpression(columnValues);
        return condition;
    }

    public static Attribute notIn(String key, double[] valueList) {
        Attribute condition = new Attribute();
        condition.key = key;
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromDouble(valueList[i]);
        }
        condition.expression = new NotInExpression(columnValues);
        return condition;
    }

    public static Attribute notIn(String key, boolean[] valueList) {
        Attribute condition = new Attribute();
        condition.key = key;
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromBoolean(valueList[i]);
        }
        condition.expression = new NotInExpression(columnValues);
        return condition;
    }

    public static Attribute inRange(String key, ColumnValue begin, ColumnValue end) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new RangeExpression(begin, end);
        return condition;
    }

    public static Attribute inRange(String key, String begin, String end) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new RangeExpression(ColumnValue.fromString(begin), ColumnValue.fromString(end));
        return condition;
    }

    public static Attribute inRange(String key, long begin, long end) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new RangeExpression(ColumnValue.fromLong(begin), ColumnValue.fromLong(end));
        return condition;
    }

    public static Attribute inRange(String key, byte[] begin, byte[] end) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new RangeExpression(ColumnValue.fromBinary(begin), ColumnValue.fromBinary(end));
        return condition;
    }

    public static Attribute inRange(String key, double begin, double end) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new RangeExpression(ColumnValue.fromDouble(begin), ColumnValue.fromDouble(end));
        return condition;
    }

    /**
     * 前缀匹配
     * @param key
     * @param value
     * @return
     */
    public static Attribute prefix(String key, String value) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new PrefixExpression(value);
        return condition;
    }

    /**
     * 模糊匹配
     * @param key
     * @param value
     * @return
     */
    public static Attribute wildcard(String key, String value) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new WildcardExpression(value);
        return condition;
    }

    /**
     * geo查询：找出落在指定多边形包围起来的图形内的数据
     * @param key
     * @param polygonList
     * @return
     */
    public static Attribute inGeoPolygon(String key, List<String> polygonList) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new GeoPolygonExpression(polygonList);
        return condition;
    }

    /**
     * geo查询：找出落在指定多边形包围起来的图形内的数据
     * @param key
     * @param polygonList
     * @return
     */
    public static Attribute inGeoPolygon(String key, String[] polygonList) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new GeoPolygonExpression(polygonList);
        return condition;
    }

    /**
     * geo查询：找出与某个位置某个距离内的数据
     * @param key
     * @param center
     * @param distanceInMeter
     * @return
     */
    public static Attribute inGeoDistance(String key, String center, double distanceInMeter) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new GeoDistanceExpression(center, distanceInMeter);
        return condition;
    }

    /**
     * geo查询：找出经纬度落在指定矩形内的数据
     * @param key
     * @param topLeftPos
     * @param bottomRightPos
     * @return
     */
    public static Attribute inGeoBoundingBox(String key, String topLeftPos, String bottomRightPos) {
        Attribute condition = new Attribute();
        condition.key = key;
        condition.expression = new GeoBoundingBoxExpression(topLeftPos, bottomRightPos);
        return condition;
    }

    public Query getQuery() {
        return  this.expression.getQuery(key);
    }
}

