package com.alicloud.openservices.tablestore.model.filter;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.condition.CompositeColumnValueCondition;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * The filter used during TableStore query operations. CompositeColumnValueFilter is used to represent logical relationship conditions between ColumnValueFilters, which mainly include NOT, AND, and OR logical relationship conditions. Among them, NOT and AND indicate binary or multi-relationship conditions, while NOT only indicates a unary relationship.
 * <p>The logical relationship is provided by the parameter in the constructor {@link CompositeColumnValueFilter#CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator)}.</p>
 * <p>If the logical relationship is {@link CompositeColumnValueFilter.LogicOperator#NOT}, you can add a ColumnValueFilter through {@link CompositeColumnValueFilter#addFilter(ColumnValueFilter)}. The added ColumnValueFilter must be exactly one.</p>
 * <p>If the logical relationship is {@link CompositeColumnValueFilter.LogicOperator#AND}, you can add a ColumnValueFilter through {@link CompositeColumnValueFilter#addFilter(ColumnValueFilter)}. The added ColumnValueFilters must be greater than or equal to two.</p>
 * <p>If the logical relationship is {@link CompositeColumnValueFilter.LogicOperator#OR}, you can add a ColumnValueFilter through {@link CompositeColumnValueFilter#addFilter(ColumnValueFilter)}. The added ColumnValueFilters must be greater than or equal to two.</p>
 */
public class CompositeColumnValueFilter extends ColumnValueFilter {

    public enum LogicOperator {
        NOT, AND, OR;
    }

    private LogicOperator type;
    private List<ColumnValueFilter> filters;

    public CompositeColumnValueFilter(LogicOperator loType) {
        Preconditions.checkNotNull(loType, "The operation type should not be null.");
        this.type = loType;
        this.filters = new ArrayList<ColumnValueFilter>();
    }

    /**
     * Add a ColumnValueFilter to the logical relationship group.
     * <p>If the logical relationship is {@link CompositeColumnValueFilter.LogicOperator#NOT}, only one ColumnValueFilter can be added.</p>
     * <p>If the logical relationship is {@link CompositeColumnValueFilter.LogicOperator#AND}, at least two ColumnValueFilters must be added.</p>
     * <p>If the logical relationship is {@link CompositeColumnValueFilter.LogicOperator#OR}, at least two ColumnValueFilters must be added.</p>
     *
     * @param filter
     * @return
     */
    public CompositeColumnValueFilter addFilter(ColumnValueFilter filter) {
        Preconditions.checkNotNull(filter, "The filter should not be null.");
        this.filters.add(filter);
        return this;
    }

    /**
     * Clear all ColumnValueFilters in the logical relationship group.
     */
    public void clear() {
        this.filters.clear();
    }

    /**
     * View the currently set logical relationship.
     *
     * @return Logical relationship symbol.
     */
    public LogicOperator getOperationType() {
        return this.type;
    }

    /**
     * Returns all ColumnValueFilters in the logical relationship group.
     *
     * @return All ColumnValueFilters.
     */
    public List<ColumnValueFilter> getSubFilters() {
        return this.filters;
    }

    public CompositeColumnValueCondition toCondition() {
        CompositeColumnValueCondition.LogicOperator logicOperator;
        switch (type) {
            case NOT:
                logicOperator = CompositeColumnValueCondition.LogicOperator.NOT;
                break;
            case AND:
                logicOperator = CompositeColumnValueCondition.LogicOperator.AND;
                break;
            case OR:
                logicOperator = CompositeColumnValueCondition.LogicOperator.OR;
                break;
            default:
                throw new ClientException("Unknown logicOperator: " + type.name());
        }
        CompositeColumnValueCondition compositeColumnValueCondition = new CompositeColumnValueCondition(logicOperator);
        for (Filter filter : getSubFilters()) {
            if (filter instanceof SingleColumnValueFilter) {
                compositeColumnValueCondition.addCondition(((SingleColumnValueFilter) filter).toCondition());
            } else if (filter instanceof CompositeColumnValueFilter) {
                compositeColumnValueCondition.addCondition(((CompositeColumnValueFilter) filter).toCondition());
            } else {
                throw new ClientException("Unknown filter type: " + filter.getFilterType());
            }
        }
        return compositeColumnValueCondition;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.COMPOSITE_COLUMN_VALUE_FILTER;
    }

    @Override
    public ByteString serialize() {
        return OTSProtocolBuilder.buildCompositeColumnValueFilter(this);
    }
}
