/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.parser;

/**
 * The exception from parsing service result.
 *
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
