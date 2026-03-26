package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

/**
 * Request for describing a knowledge base.
 * <p>
 * This class encapsulates the parameters required to retrieve detailed information
 * about an existing knowledge base.
 * </p>
 */
public class DescribeKnowledgeBaseRequest implements Request {

    /**
     * The JSON string representation of this request.
     * This field is transient and will not be serialized.
     */
    private transient String jsonStr;
    
    /**
     * The name of the knowledge base to describe.
     */
    private String knowledgeBaseName;

    /**
     * Default constructor.
     */
    public DescribeKnowledgeBaseRequest() {
    }

    /**
     * Constructs a DescribeKnowledgeBaseRequest with the specified knowledge base name.
     *
     * @param knowledgeBaseName the name of the knowledge base to describe
     */
    public DescribeKnowledgeBaseRequest(String knowledgeBaseName) {
        this.knowledgeBaseName = knowledgeBaseName;
    }

    /**
     * Gets the name of the knowledge base.
     *
     * @return the knowledge base name
     */
    public String getKnowledgeBaseName() {
        return knowledgeBaseName;
    }

    /**
     * Sets the name of the knowledge base.
     *
     * @param knowledgeBaseName the knowledge base name to set
     */
    public void setKnowledgeBaseName(String knowledgeBaseName) {
        this.knowledgeBaseName = knowledgeBaseName;
    }

    /**
     * Gets the JSON string representation of this request.
     *
     * @return the JSON string
     */
    public String getJsonStr() {
        return jsonStr;
    }

    /**
     * Sets the JSON string representation of this request.
     *
     * @param jsonStr the JSON string to set
     */
    public void setJsonStr(String jsonStr) {
        this.jsonStr = jsonStr;
    }

    /**
     * Gets the operation name for this request.
     *
     * @return the operation name "DescribeKnowledgeBase"
     */
    @Override
    public String getOperationName() {
        return OperationNames.OP_DESCRIBE_KNOWLEDGE_BASE;
    }

    /**
     * Converts this request to a JSON string.
     *
     * @return the JSON string representation of this request
     */
    public String toJson() {
        if (jsonStr != null) {
            return jsonStr;
        }
        return GsonUtils.toJson(this);
    }
}
