package com.alicloud.openservices.tablestore.model.filter;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.condition.CompositeColumnValueCondition;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * TableStore查询操作时使用的过滤器，CompositeColumnValueFilter用于表示ColumnValueFilter之间的逻辑关系条件，主要有NOT、AND和OR三种逻辑关系条件，其中NOT和AND表示二元或多元的关系，NOT只表示一元的关系。
 * <p>逻辑关系通过构造函数{@link CompositeColumnValueFilter#CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator)}的参数提供。</p>
 * <p>若逻辑关系为{@link CompositeColumnValueFilter.LogicOperator#NOT}，可以通过{@link CompositeColumnValueFilter#addFilter(ColumnValueFilter)}添加ColumnValueFilter，添加的ColumnValueFilter有且只有一个。</p>
 * <p>若逻辑关系为{@link CompositeColumnValueFilter.LogicOperator#AND}，可以通过{@link CompositeColumnValueFilter#addFilter(ColumnValueFilter)}添加ColumnValueFilter，添加的ColumnValueFilter必须大于等于两个。</p>
 * <p>若逻辑关系为{@link CompositeColumnValueFilter.LogicOperator#OR}，可以通过{@link CompositeColumnValueFilter#addFilter(ColumnValueFilter)}添加ColumnValueFilter，添加的ColumnValueFilter必须大于等于两个。</p>
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
     * 增加逻辑关系组中的ColumnValueFilter。
     * <p>若逻辑关系为{@link CompositeColumnValueFilter.LogicOperator#NOT}，有且只能添加一个ColumnValueFilter。</p>
     * <p>若逻辑关系为{@link CompositeColumnValueFilter.LogicOperator#AND}，必须添加至少两个ColumnValueFilter。</p>
     * <p>若逻辑关系为{@link CompositeColumnValueFilter.LogicOperator#OR}，必须添加至少两个ColumnValueFilter。</p>
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
     * 清空逻辑关系组中的所有ColumnValueFilter。
     */
    public void clear() {
        this.filters.clear();
    }

    /**
     * 查看当前设置的逻辑关系。
     *
     * @return 逻辑关系符号。
     */
    public LogicOperator getOperationType() {
        return this.type;
    }

    /**
     * 返回逻辑关系组中的所有ColumnValueFilter。
     *
     * @return 所有ColumnValueFilter。
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
