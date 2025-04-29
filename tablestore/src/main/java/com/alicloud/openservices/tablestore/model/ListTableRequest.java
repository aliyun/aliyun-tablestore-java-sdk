package com.alicloud.openservices.tablestore.model;

public class ListTableRequest implements Request {
    public ListTableRequest() {
    }

    public String getOperationName() {
        return OperationNames.OP_LIST_TABLE;
    }

}
