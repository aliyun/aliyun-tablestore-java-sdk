package com.alicloud.openservices.tablestore.model.sql;

import com.alicloud.openservices.tablestore.model.ColumnType;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZonedDateTime;

/**
 * Represents the data structure of a SQL row
 */
public interface SQLRow {

    /**
     * Get the data object according to the column cursor.
     *
     * @param columnIndex Column cursor
     * @return Data object
     */
    Object get(int columnIndex);

    /**
     * Get the data object by column name.
     *
     * @param name Column name
     * @return Data object
     */
    Object get(String name);

    /**
     * Get the string type value according to the column cursor.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#STRING}.</p>
     *
     * @param columnIndex Column cursor
     * @return String type value
     */
    String getString(int columnIndex);

    /**
     * Get the string type value by column name.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#STRING}.</p>
     *
     * @param name Column name
     * @return String type value
     */
    String getString(String name);

    /**
     * Get the integer value according to the column cursor.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#INTEGER}.</p>
     *
     * @param columnIndex Column cursor
     * @return Integer value
     */
    Long getLong(int columnIndex);

    /**
     * Get the integer value by column name.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#INTEGER}.</p>
     *
     * @param name Column name
     * @return Integer value
     */
    Long getLong(String name);

    /**
     * Get the value of boolean type according to the column cursor.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#BOOLEAN}.</p>
     *
     * @param columnIndex Column cursor
     * @return Value of boolean type
     */
    Boolean getBoolean(int columnIndex);

    /**
     * Get the value of boolean type according to the column name.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#BOOLEAN}.</p>
     *
     * @param name Column name
     * @return Value of boolean type
     */
    Boolean getBoolean(String name);

    /**
     * Get the value of float type according to the column cursor.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#DOUBLE}.</p>
     *
     * @param columnIndex Column cursor
     * @return Value of float type
     */
    Double getDouble(int columnIndex);

    /**
     * Get the value of float type according to the column name.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#DOUBLE}.</p>
     *
     * @param name Column name
     * @return Value of float type
     */
    Double getDouble(String name);

    /**
     * Get the value of time type according to the column cursor.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#DATETIME}.</p>
     *
     * @param columnIndex Column cursor
     * @return Returns a value of type java.time.ZonedDateTime
     */
    ZonedDateTime getDateTime(int columnIndex);

    /**
     * Get the value of time type according to the column name.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#DATETIME}.</p>
     *
     * @param name Column name
     * @return Returns a value of type java.time.ZonedDateTime
     */
    ZonedDateTime getDateTime(String name);

    /**
     * Get the value of the time interval type according to the column cursor.
     * <p>Currently, a value can only be obtained when the data type is {@link ColumnType#TIME}.</p>
     *
     * @param columnIndex Column cursor
     * @return Returns java.time.Duration type
     */
    Duration getTime(int columnIndex);

    /**
     * Get the value of time interval type according to the column name.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#TIME}.</p>
     *
     * @param name Column name
     * @return Returns java.time.Duration type
     */
    Duration getTime(String name);

    /**
     * Get the date type value according to the column cursor.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#DATE}.</p>
     *
     * @param columnIndex Column cursor
     * @return Returns java.time.LocalDate type
     */
    LocalDate getDate(int columnIndex);

    /**
     * Get the value of date type according to the column name.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#DATE}.</p>
     *
     * @param name Column name
     * @return Returns java.time.LocalDate type
     */
    LocalDate getDate(String name);

    /**
     * Get the value of Binary type according to the column cursor.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#BINARY}.</p>
     *
     * @param columnIndex Column cursor
     * @return Value of Binary type
     */
    ByteBuffer getBinary(int columnIndex);

    /**
     * Get the value of BINARY type according to the column name.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#BINARY}.</p>
     *
     * @param name Column name
     * @return The value of BINARY type
     */
    ByteBuffer getBinary(String name);

    /**
     * It is recommended to use only for Debug and Testing.
     * Format and output the data of this row in the format "columnA: valueA, columnB: valueB".
     *
     * @return formatted output
     */
    String toDebugString();

}
