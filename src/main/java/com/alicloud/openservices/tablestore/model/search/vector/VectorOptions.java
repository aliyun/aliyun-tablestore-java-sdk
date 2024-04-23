package com.alicloud.openservices.tablestore.model.search.vector;


import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

import java.util.ArrayList;
import java.util.List;

public class VectorOptions implements Jsonizable {
    /**
     * 向量的存储类型
     */
    private VectorDataType dataType;

    /**
     * 向量的维度
     */
    private Integer dimension;

    /**
     * 向量的相似度度量方式
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
