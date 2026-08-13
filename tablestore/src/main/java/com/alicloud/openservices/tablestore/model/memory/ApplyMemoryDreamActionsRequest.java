package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;
import java.util.List;

public class ApplyMemoryDreamActionsRequest extends AbstractMemoryRequest {
    private String memoryStoreName;
    private String dreamId;
    private List<String> actionIds;
    private String applier;

    public ApplyMemoryDreamActionsRequest() {
    }

    public ApplyMemoryDreamActionsRequest(String memoryStoreName, String dreamId, List<String> actionIds) {
        this.memoryStoreName = memoryStoreName;
        this.dreamId = dreamId;
        this.actionIds = actionIds;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_APPLY_MEMORY_DREAM_ACTIONS;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public String getDreamId() {
        return dreamId;
    }

    public void setDreamId(String dreamId) {
        this.dreamId = dreamId;
    }

    public List<String> getActionIds() {
        return actionIds;
    }

    public void setActionIds(List<String> actionIds) {
        this.actionIds = actionIds;
    }

    public String getApplier() {
        return applier;
    }

    public void setApplier(String applier) {
        this.applier = applier;
    }
}
