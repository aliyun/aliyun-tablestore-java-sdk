package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.List;

public class DeleteDocumentsRequest implements Request {

    private transient String jsonStr;
    private String knowledgeBaseName;
    private String subspace;
    private List<DeleteDocumentItem> documents;

    public DeleteDocumentsRequest() {
    }

    public DeleteDocumentsRequest(String knowledgeBaseName) {
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

    public List<DeleteDocumentItem> getDocuments() {
        return documents;
    }

    public void setDocuments(List<DeleteDocumentItem> documents) {
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
        return OperationNames.OP_DELETE_DOCUMENTS;
    }

    public String toJson() {
        if (jsonStr != null) {
            return jsonStr;
        }
        return GsonUtils.toJson(this);
    }

    public static class DeleteDocumentItem {
        private String docId;
        private String ossKey;

        public DeleteDocumentItem() {
        }

        public String getDocId() {
            return docId;
        }

        public void setDocId(String docId) {
            this.docId = docId;
        }

        public String getOssKey() {
            return ossKey;
        }

        public void setOssKey(String ossKey) {
            this.ossKey = ossKey;
        }
    }
}
