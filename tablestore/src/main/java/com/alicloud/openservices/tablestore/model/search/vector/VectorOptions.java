package com.alicloud.openservices.tablestore.model.search.vector;


import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

import java.util.ArrayList;
import java.util.List;

public class VectorOptions implements Jsonizable {
    /**
     * The storage type of the vector
     */
    private VectorDataType dataType;

    /**
     * Dimension of the vector
     */
    private Integer dimension;

    /**
     * Similarity measurement method for vectors
     */
    private VectorMetricType metricType;

    public VectorOptions() {
    }

    public VectorOptions(VectorDataType dataType, int dimension, VectorMetricType metricType) {
        this.dataType = dataType;
        this.dimension = dimension;
        this.metricType = metricType;
    }

    public VectorDataType getDataType() {
        return dataType;
    }

    public VectorOptions setDataType(VectorDataType dataType) {
        this.dataType = dataType;
        return this;
    }

    public Integer getDimension() {
        return dimension;
    }

    public VectorOptions setDimension(Integer dimension) {
        this.dimension = dimension;
        return this;
    }

    public VectorMetricType getMetricType() {
        return metricType;
    }

    public VectorOptions setMetricType(VectorMetricType metricType) {
        this.metricType = metricType;
        return this;
    }

    @Override
    public String jsonize() {
        StringBuilder stringBuilder = new StringBuilder();
        jsonize(stringBuilder, "\n");
        return stringBuilder.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        sb.append(newline);
        List<String> jsonItems = new ArrayList<String>();

        if (dataType != null) {
            jsonItems.add("\t\"dataType\": \"" + dataType.name() + "\"");
        }
        if (dimension != null) {
            jsonItems.add("\t\"dimension\": " + dimension);
        }
        if (metricType != null) {
            jsonItems.add("\t\"metricType\": \"" + metricType.name() + "\"");
        }
        for (int i = 0; i < jsonItems.size(); i++) {
            if (i != 0) {
                sb.append(",").append(newline);
            }
            sb.append(jsonItems.get(i));
        }
        sb.append(newline);
        sb.append('}');
    }
}
