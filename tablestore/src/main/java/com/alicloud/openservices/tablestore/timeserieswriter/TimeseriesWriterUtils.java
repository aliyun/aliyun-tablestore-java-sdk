package com.alicloud.openservices.tablestore.timeserieswriter;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesRow;

public class TimeseriesWriterUtils {

    public static void checkMeasurement(TimeseriesRow timeseriesRow) throws ClientException {
        if (timeseriesRow.getTimeseriesKey().getMeasurementName() == null || timeseriesRow.getTimeseriesKey().getMeasurementName().length() == 0) {
            throw new ClientException("The measurement of time series row should not be empty.");
        }
    }

}
