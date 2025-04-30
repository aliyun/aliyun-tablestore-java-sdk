package com.alicloud.openservices.tablestore.timestream.bench;

import com.google.gson.Gson;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;

/**
 * Usage
 * Conf conf = Conf.newInstance("src/test/resources/test_conf.json");
 */

public class Conf {
    private String endpoint;
    private String accessId;
    private String accessKey;
    private String instance;
    private Integer batchCount = 200; // Number of rows within a single request
    private Integer concurrency = 10; // Concurrency count for a single SDK connection
    private Integer bufferRows = 16 * 1024; // The maximum number of data rows buffered in memory, default is 1024 rows, and it must be a power of 2.
    private String metricPrefix = "cpu";
    private Integer metricCount = 10;

    private Conf() {}

    public static Conf newInstance(String configPath) throws FileNotFoundException {
        Reader r = new FileReader(configPath);
        Gson gson = new Gson();
        Conf c = gson.fromJson(r, Conf.class);
        return c;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public Integer getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(Integer batchCount) {
        this.batchCount = batchCount;
    }

    public Integer getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(Integer concurrency) {
        this.concurrency = concurrency;
    }

    public Integer getBufferRows() {
        return bufferRows;
    }

    public void setBufferRows(Integer bufferRows) {
        this.bufferRows = bufferRows;
    }

    public String getMetricPrefix() {
        return metricPrefix;
    }

    public void setMetricPrefix(String metricPrefix) {
        this.metricPrefix = metricPrefix;
    }

    public Integer getMetricCount() {
        return metricCount;
    }

    public void setMetricCount(Integer metricCount) {
        this.metricCount = metricCount;
    }
}
