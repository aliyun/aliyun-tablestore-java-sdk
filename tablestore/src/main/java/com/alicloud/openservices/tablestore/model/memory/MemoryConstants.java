package com.alicloud.openservices.tablestore.model.memory;

public final class MemoryConstants {
    public static final String STORAGE_MODE_OTS = "ots";
    public static final String STORAGE_MODE_FILE_OTS = "file+ots";
    public static final String STORAGE_MODE_FILE_MEMORY = "filemem";
    public static final String ITEM_TYPE_MEMORY_FILE = "memoryfile";

    public static final int MAX_ITEM_CONTENT_BYTES = 102400;
    public static final int MAX_ITEM_PATH_BYTES = 800;
    public static final int MAX_ITEMS_PER_SCOPE = 2000;

    public static final String ITEM_OPERATION_CREATED = "created";
    public static final String ITEM_OPERATION_MODIFIED = "modified";
    public static final String ITEM_OPERATION_DELETED = "deleted";

    public static final String MEMORY_TASK_STATUS_QUEUED = "queued";
    public static final String MEMORY_TASK_STATUS_RUNNING = "running";
    public static final String MEMORY_TASK_STATUS_COMPLETED = "completed";
    public static final String MEMORY_TASK_STATUS_FAILED = "failed";
    public static final String MEMORY_TASK_STATUS_NEEDS_RECONCILE = "needs_reconcile";

    public static final String DREAM_TASK_STATUS_QUEUED = "queued";
    public static final String DREAM_TASK_STATUS_RUNNING = "running";
    public static final String DREAM_TASK_STATUS_PLANNING = "planning";
    public static final String DREAM_TASK_STATUS_APPLYING = "applying";
    public static final String DREAM_TASK_STATUS_COMPLETED = "completed";
    public static final String DREAM_TASK_STATUS_COMPLETED_WITH_FAILURES = "completed_with_failures";
    public static final String DREAM_TASK_STATUS_FAILED = "failed";
    public static final String DREAM_TASK_STATUS_CANCELLED = "cancelled";

    public static final String DREAM_ACTION_STATUS_PROPOSED = "proposed";
    public static final String DREAM_ACTION_STATUS_APPLIED = "applied";
    public static final String DREAM_ACTION_STATUS_SKIPPED = "skipped";
    public static final String DREAM_ACTION_STATUS_FAILED = "failed";
    public static final String DREAM_ACTION_STATUS_NOT_FOUND = "not_found";

    public static final String DREAM_APPLY_MODE_PROPOSAL = "proposal";
    public static final String DREAM_APPLY_MODE_SAFE_AUTO = "safe_auto";

    public static final String DREAM_SCOPE_OUTPUT_MODE_PRESERVE_SCOPE = "preserve_scope";
    public static final String DREAM_SCOPE_OUTPUT_MODE_PROMOTE_SCOPE = "promote_scope";

    public static final String DREAM_TASK_TYPE_MEMORY = "memory";
    public static final String DREAM_TASK_TYPE_SKILL = "skill";
    public static final String DREAM_TASK_TYPE_PROFILE = "profile";

    public static final String DREAM_ACTION_ADD = "ADD";
    public static final String DREAM_ACTION_UPDATE = "UPDATE";
    public static final String DREAM_ACTION_DELETE = "DELETE";
    public static final String DREAM_ACTION_MERGE = "MERGE";
    public static final String DREAM_ACTION_NOOP = "NOOP";
    public static final String DREAM_ACTION_EMIT_SKILL = "EMIT_SKILL";
    public static final String DREAM_ACTION_EMIT_PROFILE = "EMIT_PROFILE";

    public static final String DREAM_SKIPPED_STALE_TARGET_MEMORY = "stale_target_memory";
    public static final String DREAM_SKIPPED_TASK_CANCELLED = "task_cancelled";
    public static final String DREAM_SKIPPED_INVALID_ACTION = "invalid_action";
    public static final String DREAM_SKIPPED_SCOPE_VIOLATION = "scope_violation";
    public static final String DREAM_SKIPPED_MISSING_SOURCE = "missing_source";
    public static final String DREAM_SKIPPED_DELETE_REQUIRES_MANUAL_APPLY = "delete_requires_manual_apply";
    public static final String DREAM_SKIPPED_PROMOTE_REQUIRES_MANUAL_APPLY = "promote_requires_manual_apply";
    public static final String DREAM_SKIPPED_CONFIDENCE_BELOW_THRESHOLD = "confidence_below_threshold";
    public static final String DREAM_SKIPPED_SUPERSEDED_BY_SKILL = "superseded_by_skill";
    public static final String DREAM_SKIPPED_SUPERSEDED_BY_PROFILE = "superseded_by_profile";
    public static final String DREAM_SKIPPED_NOOP_NOTHING_TO_APPLY = "noop_nothing_to_apply";
    public static final String DREAM_SKIPPED_CONFLICTING_ACTION = "conflicting_action";

    public static final String DREAM_ACTION_ORDER_CREATED_AT_ASC = "created_at_asc";
    public static final String DREAM_ACTION_ORDER_CREATED_AT_DESC = "created_at_desc";
    public static final String DREAM_ACTION_ORDER_CONFIDENCE_DESC = "confidence_desc";

    public static final String DREAM_CONFIDENCE_THRESHOLD_ADD = "add";
    public static final String DREAM_CONFIDENCE_THRESHOLD_UPDATE = "update";
    public static final String DREAM_CONFIDENCE_THRESHOLD_MERGE = "merge";

    private MemoryConstants() {
    }
}
