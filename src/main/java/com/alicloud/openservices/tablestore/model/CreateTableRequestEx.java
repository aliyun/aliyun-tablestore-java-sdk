package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * CreateTableRequestEx继承了CreateTableRequest，并额外提供了表预分区能力。
 */
public class CreateTableRequestEx extends CreateTableRequest {

    /**
     * 表的分区信息。
     */
    private List<PrimaryKeyValue> splitPoints = new ArrayList<PrimaryKeyValue>();

    public CreateTableRequestEx(TableMeta tableMeta, TableOptions tableOptions) {
        super(tableMeta, tableOptions);
    }

    public CreateTableRequestEx(TableMeta tableMeta, TableOptions tableOptions, ReservedThroughput reservedThroughput) {
        super(tableMeta, tableOptions, reservedThroughput);
    }

    public CreateTableRequestEx(TableMeta tableMeta, TableOptions tableOptions, List<IndexMeta> indexMeta) {
        super(tableMeta, tableOptions, indexMeta);
    }

    public CreateTableRequestEx(TableMeta tableMeta, TableOptions tableOptions, ReservedThroughput reservedThroughput, List<IndexMeta> indexMeta) {
        super(tableMeta, tableOptions, reservedThroughput, indexMeta);
    }

    public List<PrimaryKeyValue> getSplitPoints() {
        return splitPoints;
    }

    public void setSplitPoints(List<PrimaryKeyValue> splitPoints) {
        Preconditions.checkArgument(!(splitPoints == null || splitPoints.isEmpty()),
                "The split-point list cannot be empty.");

        PrimaryKeyType pkType = getTableMeta().getPrimaryKeyList().get(0).getType();
        PrimaryKeyValue lastPoint = null;

        for (PrimaryKeyValue currentPoint : splitPoints) {
            //INF_MIN和INF_MAX的type为null
            Preconditions.checkArgument(currentPoint.getType() != null,
                    "The split-point can't be set as an INF value.");
            Preconditions.checkArgument(currentPoint.getType() == pkType,
                    "The split-point's type doesn't match the partition key's type.");
            if (lastPoint != null) {
                Preconditions.checkArgument(lastPoint.compareTo(currentPoint) < 0,
                        "The split-point list isn't strictly increasing.");
            }
            lastPoint = currentPoint;
            this.splitPoints = splitPoints;
        }
    }
}
