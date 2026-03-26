package com.alicloud.openservices.tablestore.model;

public class JsonError {
    private String code;
    private String message;
    
    public JsonError() {
        // 无参构造函数，供 Gson 反序列化使用
    }
    
    public JsonError(String code, String message) {
        this.code = code;
        this.message = message;
    }
    
    public String getCode() {
        return code;
    }
    
    public void setCode(String code) {
        this.code = code;
    }
    
    public String getMessage() {
        return message;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
    
    @Override
    public String toString() {
        return "JsonError{code='" + code + "', message='" + message + "'}";
    }
}
