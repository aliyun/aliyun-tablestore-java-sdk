package com.alicloud.openservices.tablestore.model;

import java.time.Instant;

import com.alicloud.openservices.tablestore.core.protocol.globaltable.GlobalTable;

public class GlobalTableTypes {
    public static class BaseTable {
        private String regionId;
        private String instanceName;
        private String tableName;

        public BaseTable(String regionId, String instanceName, String tableName) {
            this.regionId = regionId;
            this.instanceName = instanceName;
            this.tableName = tableName;
        }

        public String getRegionId() {
            return regionId;
        }

        public void setRegionId(String regionId) {
            this.regionId = regionId;
        }

        public String getInstanceName() {
            return instanceName;
        }

        public void setInstanceName(String instanceName) {
            this.instanceName = instanceName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }
    }

    public static class Placement {
        private String regionId;
        private String instanceName;
        private boolean writable;

        public Placement(String regionId, String instanceName, boolean writable) {
            this.regionId = regionId;
            this.instanceName = instanceName;
            this.writable = writable;
        }

        public String getRegionId() {
            return regionId;
        }

        public void setRegionId(String regionId) {
            this.regionId = regionId;
        }

        public String getInstanceName() {
            return instanceName;
        }

        public void setInstanceName(String instanceName) {
            this.instanceName = instanceName;
        }

        public boolean isWritable() {
            return writable;
        }

        public void setWritable(boolean writable) {
            this.writable = writable;
        }
    }

    public static class PhyTable {
        private String regionId;
        private String instanceName;
        private String tableName;
        private PhyTableStatus status;
        private long statusTimestamp;
        private boolean writable;
        private String role;
        private String tableId;
        private SyncStage stage;
        private Instant rpo;
        //private long	MetaVersion;
        private boolean isFailed;
        private String message;

        public PhyTable(String regionId, String instanceName, String tableName, boolean writable) {
            this.regionId = regionId;
            this.instanceName = instanceName;
            this.tableName = tableName;
            this.writable = writable;
        }

        public String getRegionId() {
            return regionId;
        }

        public void setRegionId(String regionId) {
            this.regionId = regionId;
        }

        public String getInstanceName() {
            return instanceName;
        }

        public void setInstanceName(String instanceName) {
            this.instanceName = instanceName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public PhyTableStatus getStatus() {
            return status;
        }

        public void setStatus(PhyTableStatus status) {
            this.status = status;
        }

        public long getStatusTimestamp() {
            return statusTimestamp;
        }

        public void setStatusTimestamp(long statusTimestamp) {
            this.statusTimestamp = statusTimestamp;
        }

        public boolean isWritable() {
            return writable;
        }

        public void setWritable(boolean writable) {
            this.writable = writable;
        }

        public String getTableId() {
            return tableId;
        }

        public void setTableId(String tableId) {
            this.tableId = tableId;
        }

        public SyncStage getStage() {
            return stage;
        }

        public void setStage(SyncStage stage) {
            this.stage = stage;
        }

        public boolean isFailed() {
            return isFailed;
        }

        public void setFailed(boolean failed) {
            isFailed = failed;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getRole() {
            return role;
        }

        public void setRole(String role) {
            this.role = role;
        }

        public Instant getRpo() {
            return rpo;
        }

        public void setRpo(Instant rpo) {
            this.rpo = rpo;
        }
    }

    public enum SyncMode {
        ROW, COLUMN;

        public static SyncMode parseFrom(GlobalTable.SyncMode mode) {
            if (mode==GlobalTable.SyncMode.SYNC_MODE_ROW) {
                return SyncMode.ROW;
            } else if (mode==GlobalTable.SyncMode.SYNC_MODE_COLUMN) {
                return SyncMode.COLUMN;
            } else {
                throw new IllegalArgumentException("Unknown SyncMode: " + mode);
            }
        }

        public GlobalTable.SyncMode toProtocol() {
            if (this==ROW) {
                return GlobalTable.SyncMode.SYNC_MODE_ROW;
            } else if (this==COLUMN) {
                return GlobalTable.SyncMode.SYNC_MODE_COLUMN;
            } else {
                throw new IllegalArgumentException("Unknown SyncMode: " + this);
            }
        }
    }

    public enum ServeMode {
        PRIMARY_SECONDARY, PEER_TO_PEER;

        public static ServeMode parseFrom(GlobalTable.ServeMode mode) {
            if (mode==GlobalTable.ServeMode.PRIMARY_SECONDARY) {
                return ServeMode.PRIMARY_SECONDARY;
            } else if (mode==GlobalTable.ServeMode.PEER_TO_PEER) {
                return ServeMode.PEER_TO_PEER;
            } else {
                throw new IllegalArgumentException("Unknown ServeMode: " + mode);
            }
        }

        public GlobalTable.ServeMode toProtocol() {
            if (this==PRIMARY_SECONDARY) {
                return GlobalTable.ServeMode.PRIMARY_SECONDARY;
            } else if (this==PEER_TO_PEER) {
                return GlobalTable.ServeMode.PEER_TO_PEER;
            }else {
                throw new IllegalArgumentException("Unknown ServeMode: " + this);
            }
        }
    }

    public enum SyncStage {
        INIT, FULL, INCR;

        public static SyncStage parseFrom(GlobalTable.SyncStage stage) {
            if (stage==GlobalTable.SyncStage.SYNC_INIT) {
                return SyncStage.INIT;
            } else if (stage==GlobalTable.SyncStage.SYNC_FULL) {
                return SyncStage.FULL;
            } else if (stage==GlobalTable.SyncStage.SYNC_INCR) {
                return SyncStage.INCR;
            } else {
                throw new IllegalArgumentException("Unknown SyncStage: " + stage);
            }
        }

        public GlobalTable.SyncStage toProtocol() {
            if (this==INIT) {
                return GlobalTable.SyncStage.SYNC_INIT;
            } else if (this==FULL) {
                return GlobalTable.SyncStage.SYNC_FULL;
            } else if (this==INCR) {
                return GlobalTable.SyncStage.SYNC_INCR;
            } else {
                throw new IllegalArgumentException("Unknown SyncStage: " + this);
            }
        }
    }


    public enum GlobalTableStatus {
        INIT, RE_CONF, ACTIVE;

        public static GlobalTableStatus parseFrom(GlobalTable.GlobalTableStatus protoStatus) {
            if (protoStatus==GlobalTable.GlobalTableStatus.G_INIT) {
                return GlobalTableStatus.INIT;
            } else if (protoStatus==GlobalTable.GlobalTableStatus.G_RE_CONF) {
                return GlobalTableStatus.RE_CONF;
            } else if (protoStatus==GlobalTable.GlobalTableStatus.G_ACTIVE) {
                return GlobalTableStatus.ACTIVE;
            } else {
                throw new IllegalArgumentException("Unknown GlobalTableStatus: " + protoStatus);
            }
        }
    }

    public enum PhyTableStatus {
        PENDING, INIT, SYNCDATA, READY, ACTIVE, UNBINDING, UNBOUND;

        public static PhyTableStatus parseFrom(GlobalTable.PhyTableStatus protoStatus) {
            if (protoStatus==GlobalTable.PhyTableStatus.PHY_PENDING) {
                return PhyTableStatus.PENDING;
            } else if (protoStatus==GlobalTable.PhyTableStatus.PHY_INIT) {
                return PhyTableStatus.INIT;
            } else if (protoStatus==GlobalTable.PhyTableStatus.PHY_SYNCDATA) {
                return PhyTableStatus.SYNCDATA;
            } else if (protoStatus==GlobalTable.PhyTableStatus.PHY_READY) {
                return PhyTableStatus.READY;
            } else if (protoStatus==GlobalTable.PhyTableStatus.PHY_ACTIVE) {
                return PhyTableStatus.ACTIVE;
            } else if (protoStatus==GlobalTable.PhyTableStatus.PHY_UNBINDING) {
                return PhyTableStatus.UNBINDING;
            } else if (protoStatus==GlobalTable.PhyTableStatus.PHY_UNBOUND) {
                return PhyTableStatus.UNBOUND;
            } else {
                throw new IllegalArgumentException("Unknown PhyTableStatus: " + protoStatus);
            }
        }

        public GlobalTable.PhyTableStatus toProtocol() {
            if (this==PhyTableStatus.PENDING) {
                return GlobalTable.PhyTableStatus.PHY_PENDING;
            } else if (this==PhyTableStatus.INIT) {
                return GlobalTable.PhyTableStatus.PHY_INIT;
            } else if (this==PhyTableStatus.SYNCDATA) {
                return GlobalTable.PhyTableStatus.PHY_SYNCDATA;
            } else if (this==PhyTableStatus.READY) {
                return GlobalTable.PhyTableStatus.PHY_READY;
            } else if (this==PhyTableStatus.ACTIVE) {
                return GlobalTable.PhyTableStatus.PHY_ACTIVE;
            } else if (this==PhyTableStatus.UNBINDING) {
                return GlobalTable.PhyTableStatus.PHY_UNBINDING;
            } else if (this==PhyTableStatus.UNBOUND) {
                return GlobalTable.PhyTableStatus.PHY_UNBOUND;
            } else {
                throw new IllegalArgumentException("Unknown PhyTableStatus: " + this);
            }
        }
    }

    public static class UpdatePhyTable {
        private String regionId;
        private String instanceName;
        private String tableName;
        private Boolean writable;
        private Boolean primaryEligible;

        public UpdatePhyTable(String regionId, String instanceName, String tableName) {
            this.regionId = regionId;
            this.instanceName = instanceName;
            this.tableName = tableName;
        }

        public String getRegionId() {
            return regionId;
        }

        public String getInstanceName() {
            return instanceName;
        }

        public String getTableName() {
            return tableName;
        }

        public Boolean getWritable() {
            return writable;
        }

        public void setWritable(Boolean writable) {
            this.writable = writable;
        }

        public Boolean getPrimaryEligible() {
            return primaryEligible;
        }

        public void setPrimaryEligible(Boolean primaryEligible) {
            this.primaryEligible = primaryEligible;
        }
    }


    public static class Removal {
        private String regionId;
        private String instanceName;

        public Removal(String regionId, String instanceName) {
            this.regionId = regionId;
            this.instanceName = instanceName;
        }

        public String getRegionId() {
            return regionId;
        }

        public void setRegionId(String regionId) {
            this.regionId = regionId;
        }

        public String getInstanceName() {
            return instanceName;
        }

        public void setInstanceName(String instanceName) {
            this.instanceName = instanceName;
        }
    }

}
