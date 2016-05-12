package com.aliyun.openservices.ots.model.condition;

import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.protocol.OtsProtocol2;
import com.google.protobuf.ByteString;

public class ColumnConditionBuilder {

    private static OtsProtocol2.ComparatorType toComparatorType(RelationalCondition.CompareOperator operator) {
        switch (operator) {
        case EQUAL:
            return OtsProtocol2.ComparatorType.CT_EQUAL;
        case NOT_EQUAL:
            return OtsProtocol2.ComparatorType.CT_NOT_EQUAL;
        case GREATER_THAN:
            return OtsProtocol2.ComparatorType.CT_GREATER_THAN;
        case GREATER_EQUAL:
            return OtsProtocol2.ComparatorType.CT_GREATER_EQUAL;
        case LESS_THAN:
            return OtsProtocol2.ComparatorType.CT_LESS_THAN;
        case LESS_EQUAL:
            return OtsProtocol2.ComparatorType.CT_LESS_EQUAL;
        default:
            throw new IllegalArgumentException("Unknown compare operator: " + operator);
        }
    }

    public static OtsProtocol2.LogicalOperator toLogicalOperator(CompositeCondition.LogicOperator type) {
        switch (type) {
            case NOT:
                return OtsProtocol2.LogicalOperator.LO_NOT;
            case AND:
                return OtsProtocol2.LogicalOperator.LO_AND;
            case OR:
                return OtsProtocol2.LogicalOperator.LO_OR;
            default:
                throw new IllegalArgumentException("Unknown logic operation type: " + type);
        }
    }

    public static OtsProtocol2.ColumnConditionType toColumnConditionType(ColumnConditionType type) {
        switch (type) {
        case COMPOSITE_CONDITION:
            return OtsProtocol2.ColumnConditionType.CCT_COMPOSITE;
        case RELATIONAL_CONDITION:
            return OtsProtocol2.ColumnConditionType.CCT_RELATION;
        default:
            throw new IllegalArgumentException("Unknown column condition type: " + type);
        }
    }

    public static OtsProtocol2.ColumnCondition toColumnCondition(ColumnCondition cc) {
        OtsProtocol2.ColumnCondition.Builder builder = OtsProtocol2.ColumnCondition.newBuilder();
        builder.setType(toColumnConditionType(cc.getType()));
        builder.setCondition(cc.serialize());
        return builder.build();
    }

    public static ByteString buildColumnCondition(ColumnCondition cc) {
        return toColumnCondition(cc).toByteString();
    }

    public static ByteString buildCompositeCondition(CompositeCondition cc) {
        OtsProtocol2.CompositeCondition.Builder builder = OtsProtocol2.CompositeCondition.newBuilder();
        builder.setCombinator(toLogicalOperator(cc.getOperationType()));

        for (ColumnCondition c : cc.getSubConditions()) {
            builder.addSubConditions(toColumnCondition(c));
        }

        return builder.build().toByteString();
    }

    public static ByteString buildRelationalCondition(RelationalCondition scc) {
        OtsProtocol2.RelationCondition.Builder builder = OtsProtocol2.RelationCondition.newBuilder();
        builder.setColumnName(scc.getColumnName());
        builder.setComparator(toComparatorType(scc.getOperator()));
        builder.setColumnValue(OTSProtocolHelper.buildColumnValue(scc.getColumnValue()));
        builder.setPassIfMissing(scc.isPassIfMissing());

        return builder.build().toByteString();
    }
}
