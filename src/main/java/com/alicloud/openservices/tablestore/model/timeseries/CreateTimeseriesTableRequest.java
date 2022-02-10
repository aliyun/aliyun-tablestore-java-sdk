package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class CreateTimeseriesTableRequest implements Request {

    /**
     * 表的结构信息。
     */
    private TimeseriesTableMeta timeseriesTableMeta;

    /**
     * 初始化CreateTimeseriesTableRequest实例。
     * <p>表的各项均为默认值，目前只包含timeseriestablemeta
     * @param timeseriesTableMeta 表的结构信息。
     */
    public CreateTimeseriesTableRequest(TimeseriesTableMeta timeseriesTableMeta) {
        setTimeseriesTableMeta(timeseriesTableMeta);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_TIMESERIES_TABLE;
    }

    /**
     * 获取表的结构信息。
     * @return 表的结构信息
     */
    public TimeseriesTableMeta getTimeseriesTableMeta() {
        return timeseriesTableMeta;
    }

    /**
     * 设置表的结构信息。
     * @param timeseriesTableMeta 表的结构信息
     */
    public void setTimeseriesTableMeta(TimeseriesTableMeta timeseriesTableMeta) {
        Preconditions.checkNotNull(timeseriesTableMeta, "timeseriesTableMeta should not be null.");
        this.timeseriesTableMeta = timeseriesTableMeta;
    }
}
