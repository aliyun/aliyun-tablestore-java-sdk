package com.alicloud.openservices.tablestore.ecosystem;

import java.io.*;

public class FilterPushdownConfig implements Serializable {

    public boolean pushRangeLong;
    public boolean pushRangeString;

    public FilterPushdownConfig() {
        pushRangeLong = true;
        pushRangeString = true;
    }

    public FilterPushdownConfig(boolean pushRangeLong, boolean pushRangeString) {
        this.pushRangeLong = pushRangeLong;
        this.pushRangeString = pushRangeString;
    }


}
