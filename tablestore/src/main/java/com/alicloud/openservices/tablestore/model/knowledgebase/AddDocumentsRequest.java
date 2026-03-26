package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.List;
import java.util.Map;

public class AddDocumentsRequest implements Request {

    private transient String jsonStr;
    private String knowledgeBaseName;
    private String subspace;
    private List<DocumentItem> documents;

    public AddDocumentsRequest() {
    }

    public AddDocumentsRequest(String knowledgeBaseName) {
        this.knowledgeBaseName = knowledgeBaseName;
    }

    public String getKnowledgeBaseName() {
        return knowledgeBaseName;
    }

    public void setKnowledgeBaseName(String knowledgeBaseName) {
        this.knowledgeBaseName = knowledgeBaseName;
    }

    public String getSubspace() {
        return subspace;
    }

    public void setSubspace(String subspace) {
        this.subspace = subspace;
    }

    public List<DocumentItem> getDocuments() {
        return documents;
    }

    public void setDocuments(List<DocumentItem> documents) {
        this.documents = documents;
    }

    public String getJsonStr() {
        return jsonStr;
    }

    public void setJsonStr(String jsonStr) {
        this.jsonStr = jsonStr;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_ADD_DOCUMENTS;
    }

    public String toJson() {
        if (jsonStr != null) {
            return jsonStr;
        }
        return GsonUtils.toJson(this);
    }

    public static class DocumentItem {
        private String ossKey;
        private Map<String, Object> metadata;
        private List<String> inclusionFilters;
        private List<String> exclusionFilters;

        public DocumentItem() {
        }

        public DocumentItem(String ossKey) {
            this.ossKey = ossKey;
        }

        public String getOssKey() {
            return ossKey;
        }

        public void setOssKey(String ossKey) {
            this.ossKey = ossKey;
        }

        public Map<String, Object> getMetadata() {
            return metadata;
        }

        public void setMetadata(Map<String, Object> metadata) {
            this.metadata = metadata;
        }

        public List<String> getInclusionFilters() {
            return inclusionFilters;
        }
        public void setInclusionFilters(List<String> inclusionFilters) {
            this.inclusionFilters = inclusionFilters;
        }

        public List<String> getExclusionFilters() {
            return exclusionFilters;
        }

        public void setExclusionFilters(List<String> exclusionFilters) {
            this.exclusionFilters = exclusionFilters;
        }

    }
}
