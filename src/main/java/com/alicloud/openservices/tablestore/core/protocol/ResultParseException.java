package com.alicloud.openservices.tablestore.core.protocol;

/**
 * The exception from parsing service result.
 */
public class ResultParseException extends Exception {
    private static final long serialVersionUID = -6660159156997037589L;

    public ResultParseException(){
        super();
    }
    
    public ResultParseException(String message){
        super(message);
    }
    
    public ResultParseException(Throwable cause){
        super(cause);
    }
    
    public ResultParseException(String message, Throwable cause){
        super(message, cause);
    }
}
