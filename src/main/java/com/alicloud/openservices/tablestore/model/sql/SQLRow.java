package com.alicloud.openservices.tablestore.model.sql;

import com.alicloud.openservices.tablestore.model.ColumnType;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.Duration;
import java.time.ZonedDateTime;

/**
 * 表示 SQL 行的数据结构
 **/
public interface SQLRow {

    /**
     * 根据列游标获取数据对象。
     *
     * @param columnIndex 列游标
     * @return 数据对象
     */
    Object get(int columnIndex);

    /**
     * 根据列名获取数据对象。
     *
     * @param name 列名
     * @return 数据对象
     */
    Object get(String name);

    /**
     * 根据列游标获取字符串类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#STRING}才能获取到值。</p>
     *
     * @param columnIndex 列游标
     * @return 字符串类型的值
     */
    String getString(int columnIndex);

    /**
     * 根据列名获取字符串类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#STRING}才能获取到值。</p>
     *
     * @param name 列名
     * @return 字符串类型的值
     */
    String getString(String name);

    /**
     * 根据列游标获取整型的值。
     * <p>当前仅当数据类型为{@link ColumnType#INTEGER}才能获取到值。</p>
     *
     * @param columnIndex 列游标
     * @return 整型的值
     */
    Long getLong(int columnIndex);

    /**
     * 根据列名获取整型的值。
     * <p>当前仅当数据类型为{@link ColumnType#INTEGER}才能获取到值。</p>
     *
     * @param name 列名
     * @return 整型的值
     */
    Long getLong(String name);

    /**
     * 根据列游标获取布尔类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#BOOLEAN}才能获取到值。</p>
     *
     * @param columnIndex 列游标
     * @return 布尔类型的值
     */
    Boolean getBoolean(int columnIndex);

    /**
     * 根据列名获取布尔类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#BOOLEAN}才能获取到值。</p>
     *
     * @param name 列名
     * @return 布尔类型的值
     */
    Boolean getBoolean(String name);

    /**
     * 根据列游标获取浮点数类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#DOUBLE}才能获取到值。</p>
     *
     * @param columnIndex 列游标
     * @return 浮点数类型的值
     */
    Double getDouble(int columnIndex);

    /**
     * 根据列名获取浮点数类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#DOUBLE}才能获取到值。</p>
     *
     * @param name 列名
     * @return 浮点数类型的值
     */
    Double getDouble(String name);

    /**
     * 根据列游标获取时间类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#DATETIME}才能获取到值。</p>
     *
     * @param columnIndex 列游标
     * @return 返回java.time.ZonedDateTime类型的值
     */
    ZonedDateTime getDateTime(int columnIndex);

    /**
     * 根据列名获取时间类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#DATETIME}才能获取到值。</p>
     *
     * @param name 列名
     * @return 返回java.time.ZonedDateTime类型的值类型的值
     */
    ZonedDateTime getDateTime(String name);

    /**
     * 根据列游标获取时间间隔类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#TIME}才能获取到值。</p>
     *
     * @param columnIndex 列游标
     * @return 返回java.time.Duration类型
     */
    Duration getTime(int columnIndex);

    /**
     * 根据列名获取时间间隔类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#TIME}才能获取到值。</p>
     *
     * @param name 列名
     * @return 返回java.time.Duration类型
     */
    Duration getTime(String name);

    /**
     * 根据列游标获取日期类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#DATE}才能获取到值。</p>
     *
     * @param columnIndex 列游标
     * @return 返回java.time.LocalDate类型
     */
    LocalDate getDate(int columnIndex);

    /**
     * 根据列名获取日期类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#DATE}才能获取到值。</p>
     *
     * @param name 列名
     * @return 返回java.time.LocalDate类型
     */
    LocalDate getDate(String name);

    /**
     * 根据列游标获取 Binary 类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#BINARY}才能获取到值。</p>
     *
     * @param columnIndex 列游标
     * @return Binary 类型的值
     */
    ByteBuffer getBinary(int columnIndex);

    /**
     * 根据列名获取 BINARY 类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#BINARY}才能获取到值。</p>
     *
     * @param name 列名
     * @return BINARY 类型的值
     */
    ByteBuffer getBinary(String name);

    /**
     * 建议只用于 Debug 和 测试使用
     * 格式化输出该行的数据，按"columnA: valueA, columnB: valueB"的格式
     *
     * @return 格式化输出
     */
    String toDebugString();

}
