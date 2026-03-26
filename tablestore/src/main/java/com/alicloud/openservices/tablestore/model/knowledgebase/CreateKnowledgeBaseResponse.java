package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.model.Response;

/**
 * Response for creating a knowledge base.
 * <p>
 * This class encapsulates the response returned after creating a knowledge base,
 * including the status code and message.
 * </p>
 */
public class CreateKnowledgeBaseResponse extends Response {
    /**
     * The response code indicating the status of the operation.
     */
    private String code;
    
    /**
     * The response message providing additional information about the operation.
     */
    private String message;

    /**
     * Default constructor.
     */
    public CreateKnowledgeBaseResponse() {
    }

    /**
     * Gets the response code.
     *
     * @return the response code
     */
    public String getCode() {
        return code;
    }

    /**
     * Sets the response code.
     *
     * @param code the response code to set
     */
    public void setCode(String code) {
        this.code = code;
    }

    /**
     * Gets the response message.
     *
     * @return the response message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Sets the response message.
     *
     * @param message the response message to set
     */
    public void setMessage(String message) {
        this.message = message;
    }
}
