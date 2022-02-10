package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.model.ColumnValue;

import java.util.List;

public class Filter {

    /**
     * Compare operator
     */
    public enum CompareOperator {
        /**
         * Empty filter
         */
        EMPTY_FILTER,

        /**
         * Equal
         */
        EQUAL,

        /**
         * >=
         */
        GREATER_EQUAL,

        /**
         * >
         */
        GREATER_THAN,

        /**
         * <=
         */
        LESS_EQUAL,

        /**
         * <
         */
        LESS_THAN,

        /**
         * !=
         */
        NOT_EQUAL,

        /**
         * A*
         */
        START_WITH,

        IN,

        IS_NULL,

        // IS_NOT_NULL;
    }

    /**
     * LogicOperator
     */
    public enum LogicOperator {
        /**
         * NOT
         */
        NOT,

        /**
         * AND
         */
        AND,

        /**
         * OR
         */
        OR;
    }

    private LogicOperator logicOperator;
    private CompareOperator compareOperator;
    private List<Filter> filters;
    private String columnName;
    private ColumnValue columnValue;
    private List<ColumnValue> columnValuesForInOperator;

    public boolean isNested() {
        return filters != null && filters.size() > 0;
    }

    public String getColumnName() {
        return columnName;
    }

    public ColumnValue getColumnValue() {
        return columnValue;
    }

    public List<Filter> getSubFilters() {
        return filters;
    }

    public LogicOperator getLogicOperator() {
        return logicOperator;
    }

    public CompareOperator getCompareOperator() {
        return compareOperator;
    }

    public Filter(LogicOperator lo, List<Filter> subFilters) {
        this.logicOperator = lo;
        this.filters = subFilters;
    }

    //constructor for "IS_NULL" "IS_NOT_NULL"
    public Filter(CompareOperator compareOperator, String columnName) {
        this.compareOperator = compareOperator;
        this.columnName = columnName;
    }

    public Filter(CompareOperator co, String columnName, ColumnValue columnValue) {
        this.compareOperator = co;
        this.columnName = columnName;
        this.columnValue = columnValue;
    }

    //constructor for "IN"
    public Filter(CompareOperator co, String columnName, List<ColumnValue> columnValuesForInOperator) {
        this.compareOperator = co;
        this.columnName = columnName;
        this.columnValuesForInOperator = columnValuesForInOperator;
    }

    public Filter(CompareOperator co) {
        this.compareOperator = co;
    }

    public static Filter emptyFilter() {
        return new Filter(CompareOperator.EMPTY_FILTER);
    }

    public void setLogicOperator(LogicOperator logicOperator) {
        this.logicOperator = logicOperator;
    }

    public void setCompareOperator(CompareOperator compareOperator) {
        this.compareOperator = compareOperator;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setColumnValue(ColumnValue columnValue) {
        this.columnValue = columnValue;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public List<ColumnValue> getColumnValuesForInOperator() {
        return columnValuesForInOperator;
    }

    public void setColumnValuesForInOperator(List<ColumnValue> columnValuesForInOperator) {
        this.columnValuesForInOperator = columnValuesForInOperator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Filter filter = (Filter) o;

        if (logicOperator != filter.logicOperator) {
            return false;
        }
        if (compareOperator != filter.compareOperator) {
            return false;
        }
        if (filters != null ? !filters.equals(filter.filters) : filter.filters != null) {
            return false;
        }
        if (columnName != null ? !columnName.equals(filter.columnName) : filter.columnName != null) {
            return false;
        }
        if (columnValue != null ? !columnValue.equals(filter.columnValue) : filter.columnValue != null) {
            return false;
        }
        return columnValuesForInOperator != null ? columnValuesForInOperator.equals(filter.columnValuesForInOperator) : filter.columnValuesForInOperator == null;
    }

    @Override
    public int hashCode() {
        int result = logicOperator != null ? logicOperator.hashCode() : 0;
        result = 31 * result + (compareOperator != null ? compareOperator.hashCode() : 0);
        result = 31 * result + (filters != null ? filters.hashCode() : 0);
        result = 31 * result + (columnName != null ? columnName.hashCode() : 0);
        result = 31 * result + (columnValue != null ? columnValue.hashCode() : 0);
        result = 31 * result + (columnValuesForInOperator != null ? columnValuesForInOperator.hashCode() : 0);
        return result;
    }
}
