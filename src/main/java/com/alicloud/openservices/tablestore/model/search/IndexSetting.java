package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.Objects;

import java.util.List;

/**
 * index的设置，主要包括分片数，路由规则设置等
 */
public class IndexSetting implements Jsonizable {

    /**
     * 自定义路由字段
     * <p>注意：默认为空。如果不熟悉该功能，请咨询开发人员或者提工单询问</p>
     */
    private List<String> routingFields;

    public IndexSetting() {
    }

    public List<String> getRoutingFields() {
        return routingFields;
    }

    public void setRoutingFields(List<String> routingFields) {
        this.routingFields = routingFields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof IndexSetting)) {
            return false;
        }
        IndexSetting setting = (IndexSetting) o;
        return Objects.equals(routingFields, setting.routingFields);
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        if (routingFields != null) {
            sb.append("\"RoutingFields\": [");
            for (int i = 0; i < routingFields.size(); i++) {
                String sourceField = routingFields.get(i);
                sb.append("\"").append(sourceField).append("\"");
                if (i != routingFields.size() - 1) {
                    sb.append(", ");
                }
            }
            sb.append("]");
        }
        sb.append("}");
    }
}
