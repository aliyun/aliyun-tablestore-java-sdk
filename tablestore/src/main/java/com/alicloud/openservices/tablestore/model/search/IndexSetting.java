package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.Objects;

import java.util.List;

/**
 * Settings for the index, mainly including the number of shards, routing rule settings, etc.
 */
public class IndexSetting implements Jsonizable {

    /**
     * Custom routing field
     * <p>Note: It is empty by default. If you are not familiar with this feature, please consult the developer or submit a ticket for inquiry.</p>
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
