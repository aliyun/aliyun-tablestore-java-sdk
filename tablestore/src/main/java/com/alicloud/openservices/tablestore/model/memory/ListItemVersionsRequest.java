package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

/**
 * Request for listing item versions within a scope.
 *
 * <p>{@code itemId} is an optional filter. When it is omitted, versions are
 * listed using the scope and any other filters configured on this request.
 * Call {@link #setItemId(String)} to restrict the result to one item.</p>
 */
public class ListItemVersionsRequest extends ItemRequest {
    private String itemId;
    private String sessionId;
    private String createdAtGte;
    private String createdAtLte;
    private String operation;
    private String view;
    private String nextToken;
    private Integer limit;

    public ListItemVersionsRequest() {
    }

    /**
     * Creates a request without an item ID filter.
     *
     * @param memoryStoreName memory store name
     * @param scope scope to query; wildcard fields are supported by the service
     * @see #setItemId(String)
     */
    public ListItemVersionsRequest(String memoryStoreName, Scope scope) {
        super(memoryStoreName, scope);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_LIST_ITEM_VERSIONS;
    }

    public String getItemId() {
        return itemId;
    }

    /**
     * Sets the optional item ID filter. Leave it unset to list versions across
     * all matching items in the requested scope.
     *
     * @param itemId item ID to filter by
     */
    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getCreatedAtGte() {
        return createdAtGte;
    }

    public void setCreatedAtGte(String createdAtGte) {
        this.createdAtGte = createdAtGte;
    }

    public String getCreatedAtLte() {
        return createdAtLte;
    }

    public void setCreatedAtLte(String createdAtLte) {
        this.createdAtLte = createdAtLte;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getView() {
        return view;
    }

    public void setView(String view) {
        this.view = view;
    }
}
