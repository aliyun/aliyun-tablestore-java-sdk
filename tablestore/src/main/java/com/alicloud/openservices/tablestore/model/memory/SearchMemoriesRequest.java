package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;
import java.util.Map;

public class SearchMemoriesRequest extends AbstractMemoryRequest {
    private String memoryStoreName;
    private Scope scope;
    private String query;
    private Map<String, String> metadata;
    private Integer topK;
    private Boolean enableRerank;
    private Scope contextScope;
    private Double minSimilarity;
    private Boolean includeEvidence;

    public SearchMemoriesRequest() {
    }

    public SearchMemoriesRequest(String memoryStoreName, String query) {
        this.memoryStoreName = memoryStoreName;
        this.query = query;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_SEARCH_MEMORIES;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public Integer getTopK() {
        return topK;
    }

    public void setTopK(Integer topK) {
        this.topK = topK;
    }

    public Boolean getEnableRerank() {
        return enableRerank;
    }

    public void setEnableRerank(Boolean enableRerank) {
        this.enableRerank = enableRerank;
    }

    public Scope getContextScope() {
        return contextScope;
    }

    public void setContextScope(Scope contextScope) {
        this.contextScope = contextScope;
    }

    public Double getMinSimilarity() {
        return minSimilarity;
    }

    public void setMinSimilarity(Double minSimilarity) {
        this.minSimilarity = minSimilarity;
    }

    public Boolean getIncludeEvidence() {
        return includeEvidence;
    }

    public void setIncludeEvidence(Boolean includeEvidence) {
        this.includeEvidence = includeEvidence;
    }
}
