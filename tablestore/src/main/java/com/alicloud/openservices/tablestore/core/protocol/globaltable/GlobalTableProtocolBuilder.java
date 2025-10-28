package com.alicloud.openservices.tablestore.core.protocol.globaltable;

import java.util.List;

import com.alicloud.openservices.tablestore.model.BindGlobalTableRequest;
import com.alicloud.openservices.tablestore.model.GlobalTableTypes.UpdatePhyTable;
import com.alicloud.openservices.tablestore.model.UpdateGlobalTableRequest;
import com.alicloud.openservices.tablestore.model.CreateGlobalTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeGlobalTableRequest;
import com.alicloud.openservices.tablestore.model.GlobalTableTypes.BaseTable;
import com.alicloud.openservices.tablestore.model.GlobalTableTypes.PhyTable;
import com.alicloud.openservices.tablestore.model.GlobalTableTypes.Placement;
import com.alicloud.openservices.tablestore.model.GlobalTableTypes.Removal;
import com.alicloud.openservices.tablestore.model.UnbindGlobalTableRequest;
import com.google.common.base.Strings;

public class GlobalTableProtocolBuilder {
    public static GlobalTable.CreateGlobalTableRequest buildCreateGlobalTableRequest(CreateGlobalTableRequest req) {
        GlobalTable.CreateGlobalTableRequest.Builder builder = GlobalTable.CreateGlobalTableRequest.newBuilder();

        // required BaseTable baseTable = 2;
        BaseTable reqBaseTable = req.getBaseTable();
        if (reqBaseTable==null) {
            throw new IllegalArgumentException("Base table is null");
        }
        GlobalTable.BaseTable.Builder protoBase = GlobalTable.BaseTable.newBuilder();
        protoBase.setRegionId(reqBaseTable.getRegionId());
        protoBase.setInstanceName(reqBaseTable.getInstanceName());
        protoBase.setTableName(reqBaseTable.getTableName());
        builder.setBaseTable(protoBase);

        // repeated Placement placements = 3;
        List<Placement> reqPlacements = req.getPlacements();
        if (reqPlacements!=null) {
            for (Placement reqPlacement : reqPlacements) {
                GlobalTable.Placement.Builder protoPlacement = GlobalTable.Placement.newBuilder();
                protoPlacement.setRegionId(reqPlacement.getRegionId());
                protoPlacement.setInstanceName(reqPlacement.getInstanceName());
                protoPlacement.setWritable(reqPlacement.isWritable());
                builder.addPlacements(protoPlacement);
            }
        }

        // required SyncMode syncMode = 4;
        builder.setSyncMode(req.getSyncMode().toProtocol());

        if (req.getServeMode()!=null) {
            builder.setServeMode(req.getServeMode().toProtocol());
        }
        return builder.build();
    }

    public static GlobalTable.BindGlobalTableRequest buildBindGlobalTableRequest(BindGlobalTableRequest req) {
        GlobalTable.BindGlobalTableRequest.Builder builder = GlobalTable.BindGlobalTableRequest.newBuilder();

        // required string globalTableId = 2;
        builder.setGlobalTableId(req.getGlobalTableId());
        // required string globalTableName = 3;
        builder.setGlobalTableName(req.getGlobalTableName());

        List<Placement> reqPlacements = req.getPlacements();
        if (reqPlacements!=null) {
            for (Placement reqPlacement : reqPlacements) {
                GlobalTable.Placement.Builder protoPlacement = GlobalTable.Placement.newBuilder();
                protoPlacement.setRegionId(reqPlacement.getRegionId());
                protoPlacement.setInstanceName(reqPlacement.getInstanceName());
                protoPlacement.setWritable(reqPlacement.isWritable());
                builder.addPlacements(protoPlacement);
            }
        }
        return builder.build();
    }

    public static GlobalTable.UnbindGlobalTableRequest buildUnbindGlobalTableRequest(UnbindGlobalTableRequest req) {
        GlobalTable.UnbindGlobalTableRequest.Builder builder = GlobalTable.UnbindGlobalTableRequest.newBuilder();

        // required string globalTableId = 2;
        builder.setGlobalTableId(req.getGlobalTableId());
        // required string globalTableName = 3;
        builder.setGlobalTableName(req.getGlobalTableName());

        List<Removal> reqRemovals = req.getRemovals();
        if (reqRemovals==null || reqRemovals.isEmpty()) {
            throw new IllegalArgumentException("Removals is null or empty");
        }

        for (Removal reqRemoval : reqRemovals) {
            GlobalTable.Removal.Builder protoRemoval = GlobalTable.Removal.newBuilder();
            protoRemoval.setRegionId(reqRemoval.getRegionId());
            protoRemoval.setInstanceName(reqRemoval.getInstanceName());
            builder.addRemovals(protoRemoval);
        }
        return builder.build();
    }

    public static GlobalTable.DescribeGlobalTableRequest buildDescribeGlobalTableRequest(DescribeGlobalTableRequest req) {
        GlobalTable.DescribeGlobalTableRequest.Builder builder = GlobalTable.DescribeGlobalTableRequest.newBuilder();
        // optional string globalTableId = 1;
        if (!Strings.isNullOrEmpty(req.getGlobalTableId())) {
            builder.setGlobalTableId(req.getGlobalTableId());
        }
        // required string globalTableName = 2;
        if (Strings.isNullOrEmpty(req.getGlobalTableName())) {
            throw new IllegalArgumentException("GlobalTableName is null or empty");
        }
        builder.setGlobalTableName(req.getGlobalTableName());

        PhyTable sdkPhyTable = req.getPhyTable();
        if (sdkPhyTable!=null) {
            GlobalTable.PhyTable.Builder protoPhyTable = GlobalTable.PhyTable.newBuilder();
            protoPhyTable.setRegionId(sdkPhyTable.getRegionId());
            protoPhyTable.setInstanceName(sdkPhyTable.getInstanceName());
            protoPhyTable.setTableName(sdkPhyTable.getTableName());
            protoPhyTable.setWritable(sdkPhyTable.isWritable());
            builder.setPhyTable(protoPhyTable);
        }
        builder.setReturnRpo(req.isReturnRpo());
        return builder.build();
    }

    public static GlobalTable.UpdateGlobalTableRequest buildUpdateGlobalTableRequest(UpdateGlobalTableRequest req) {
        GlobalTable.UpdateGlobalTableRequest.Builder builder = GlobalTable.UpdateGlobalTableRequest.newBuilder();
        // required string globalTableId = 1;
        if (Strings.isNullOrEmpty(req.getGlobalTableId())) {
            throw new IllegalArgumentException("GlobalTableId is null or empty");
        }
        builder.setGlobalTableId(req.getGlobalTableId());

        // required string globalTableName = 2;
        if (Strings.isNullOrEmpty(req.getGlobalTableName())) {
            throw new IllegalArgumentException("GlobalTableName is null or empty");
        }
        builder.setGlobalTableName(req.getGlobalTableName());

        // optional UpdatePhyTable phyTable = 3; (treated as required)
        UpdatePhyTable sdkPhyTable = req.getUpdatePhyTable();
        if (sdkPhyTable==null) {
            throw new IllegalArgumentException("PhyTable is null or empty");
        }
        GlobalTable.UpdatePhyTable.Builder protoPhyTable = GlobalTable.UpdatePhyTable.newBuilder();
        protoPhyTable.setRegionId(sdkPhyTable.getRegionId());
        protoPhyTable.setInstanceName(sdkPhyTable.getInstanceName());
        protoPhyTable.setTableName(sdkPhyTable.getTableName());
        if (sdkPhyTable.getWritable()!=null) {
            protoPhyTable.setWritable(sdkPhyTable.getWritable());
        }
        if (sdkPhyTable.getPrimaryEligible()!=null) {
            protoPhyTable.setPrimaryEligible(sdkPhyTable.getPrimaryEligible());
        }

        builder.setPhyTable(protoPhyTable);

        return builder.build();
    }
}
