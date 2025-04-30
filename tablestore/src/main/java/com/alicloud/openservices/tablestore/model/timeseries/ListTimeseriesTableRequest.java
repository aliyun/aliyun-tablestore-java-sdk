package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;



public class ListTimeseriesTableRequest implements Request {


    public ListTimeseriesTableRequest() {
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_LIST_TIMESERIES_TABLE;
    }

}
