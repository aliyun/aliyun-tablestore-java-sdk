package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.List;

/**
 * Response for getting a document from a knowledge base.
 * <p>
 * This class encapsulates the response returned after retrieving document information.
 * </p>
 */
public class GetDocumentResponse extends Response {
    /**
     * The response code indicating the status of the operation.
     */
    private String code;
    
    /**
     * The response message providing additional information about the operation.
     */
    private String message;
    
    /**
     * The list of document information.
     */
    private List<DocumentInfo> data;

    /**
     * Default constructor.
     */
    public GetDocumentResponse() {
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

    /**
     * Gets the list of document information.
     *
     * @return the list of document information
     */
    public List<DocumentInfo> getData() {
        return data;
    }

    /**
     * Sets the list of document information.
     *
     * @param data the list of document information to set
     */
    public void setData(List<DocumentInfo> data) {
        this.data = data;
    }
}
