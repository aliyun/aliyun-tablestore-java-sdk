package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class GetDocumentRequest implements Request {

    private transient String jsonStr;
    private String knowledgeBaseName;
    private String subspace;
    private String docId;
    private String ossKey;

    public GetDocumentRequest() {
    }

    public GetDocumentRequest(String knowledgeBaseName) {
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

    public String getJsonStr() {
        return jsonStr;
    }

    public void setJsonStr(String jsonStr) {
        this.jsonStr = jsonStr;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_GET_DOCUMENT;
    }

    public String toJson() {
        if (jsonStr != null) {
            return jsonStr;
        }
        return GsonUtils.toJson(this);
    }
}
