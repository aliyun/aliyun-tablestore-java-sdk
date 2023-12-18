package com.alicloud.openservices.tablestore.model.search.vector;

import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.google.protobuf.ByteString;

public class HNSWIndexParameter implements VectorIndexParameter {
    private Integer m;
    private Integer efConstruction;

    public HNSWIndexParameter() {
    }

    public HNSWIndexParameter(Integer m, Integer efConstruction) {
        this.m = m;
        this.efConstruction = efConstruction;
    }

    public Integer getM() {
        return m;
    }

    public void setM(Integer m) {
        this.m = m;
    }

    public Integer getEfConstruction() {
        return efConstruction;
    }

    public void setEfConstruction(Integer efConstruction) {
        this.efConstruction = efConstruction;
    }

    @Override
    public ByteString serialize() {
        Search.HNSWIndexParameter.Builder builder = Search.HNSWIndexParameter.newBuilder();
        if (m != null) {
            builder.setM(m);
        }
        if (efConstruction != null) {
            builder.setEfConstruction(efConstruction);
        }
        return builder.build().toByteString();
    }

    @Override
    public String jsonize() {
        StringBuilder stringBuilder = new StringBuilder();
        jsonize(stringBuilder, "");
        return stringBuilder.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        sb.append(newline);
        sb.append("\"m\": ").append(m).append(",");
        sb.append(newline);
        sb.append("\"efConstruction\": ").append(efConstruction);
        sb.append(newline);
        sb.append('}');
    }
}
