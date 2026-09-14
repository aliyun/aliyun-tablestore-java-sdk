package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

/**
 * Asynchronously and physically deletes every historical version of the file items in a wildcard scope.
 *
 * <p>The service accepts only {@code type}, {@code memoryStoreName} and {@code scope}; sending any other field
 * fails with OTSResponseFail. That is why this request adds no fields of its own — narrowing the deletion by
 * item, path or version is not supported.
 *
 * <p>{@code scope} must contain a wildcard: {@code appId} and {@code tenantId} have to be concrete, and once a
 * level is {@code *} every level to its right must be {@code *} too. The current items of the same scope must
 * already be deleted, otherwise the service answers OTSOperationConflict.
 */
public class DeleteItemVersionsRequest extends ItemRequest {

    public DeleteItemVersionsRequest() {
    }

    public DeleteItemVersionsRequest(String memoryStoreName, Scope scope) {
        super(memoryStoreName, scope);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_ITEM_VERSIONS;
    }
}
