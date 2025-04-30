package com.alicloud.openservices.tablestore.model;

public abstract class ExtensionRequest implements Request {
    RequestExtension extension;
    public RequestExtension getExtension() {return extension;}
    public void setExtension(RequestExtension extension) {this.extension = extension;}
}
