package com.alicloud.openservices.tablestore.core.utils;

public interface Jsonizable {
    public String jsonize();
    public void jsonize(StringBuilder sb, String newline);
}
