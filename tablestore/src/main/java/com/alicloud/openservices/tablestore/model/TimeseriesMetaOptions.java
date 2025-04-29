package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TimeseriesMetaOptions implements Jsonizable {

    /**
     * The TTL (Time-To-Live) for the timeline metadata, in seconds. A value of -1 indicates that it never expires.
     * Currently, when enabling the TTL feature for timeline metadata, the allowUpdateAttributes configuration must be set to false. 
     * In this case, the server will no longer allow updates to the metadata attributes of the timeline.
     */
    private OptionalValue<Integer> metaTimeToLive = new OptionalValue<Integer>("MetaTimeToLive");

    /**
     * Whether to allow updating the timeline metadata properties, needs to be set to false when the metadata TTL function is enabled.
     */
    private OptionalValue<Boolean> allowUpdateAttributes = new OptionalValue<Boolean>("AllowUpdateAttributes");

    public TimeseriesMetaOptions() {
    }

    public boolean hasSetMetaTimeToLive() {
        return this.metaTimeToLive.isValueSet();
    }

    public int getMetaTimeToLive() {
        if (!metaTimeToLive.isValueSet()) {
            throw new IllegalStateException("The value of MetaTimeToLive is not set.");
        }
        return metaTimeToLive.getValue();
    }

    public void setMetaTimeToLive(int metaTimeToLive) {
        Preconditions.checkArgument(metaTimeToLive > 0 || metaTimeToLive == -1,
                "The value of metaTimeToLive can be -1 or any positive value.");
        this.metaTimeToLive.setValue(metaTimeToLive);
    }

    public boolean hasSetAllowUpdateAttributes() {
        return this.allowUpdateAttributes.isValueSet();
    }

    public boolean getAllowUpdateAttributes() {
        if (!allowUpdateAttributes.isValueSet()) {
            throw new IllegalStateException("The value of AllowUpdateAttributes is not set.");
        }
        return allowUpdateAttributes.getValue();
    }

    public void setAllowUpdateAttributes(boolean allowUpdateAttributes) {
        this.allowUpdateAttributes.setValue(allowUpdateAttributes);
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
        boolean firstItem = true;
        if (metaTimeToLive.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"MetaTimeToLive\": ");
            sb.append(this.metaTimeToLive.getValue());
        }
        if (allowUpdateAttributes.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"AllowUpdateAttributes\": ");
            sb.append(this.allowUpdateAttributes.getValue());
        }
        sb.append("}");
    }
}
