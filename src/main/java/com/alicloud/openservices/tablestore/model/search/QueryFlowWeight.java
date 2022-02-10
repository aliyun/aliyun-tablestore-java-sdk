package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class QueryFlowWeight implements Jsonizable {
    /**
     * index name
     */
    private String indexName;

    /**
     * query flow weight, should be within range [0, 100]
     */
    private Integer weight;


    public QueryFlowWeight(String indexName, Integer weight) {
        this.indexName = indexName;
        this.weight = weight;
    }

    public String getIndexName() {
        return indexName;
    }

    public QueryFlowWeight setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public Integer getWeight() {
        return weight;
    }

    public QueryFlowWeight setWeight(Integer weight) {
        this.weight = weight;
        return this;
    }


    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append("{");
        sb.append(newline);

        sb.append("\"IndexName\": \"");
        sb.append(indexName);
        sb.append("\"");
        sb.append(",");
        sb.append(newline);

        sb.append("\"Weight\": ");
        sb.append(weight.intValue());
        sb.append(newline);

        sb.append("}");
    }
}
