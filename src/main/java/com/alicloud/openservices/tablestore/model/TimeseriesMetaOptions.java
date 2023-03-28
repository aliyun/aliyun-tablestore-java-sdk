package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TimeseriesMetaOptions implements Jsonizable {

    /**
     * 时间线元数据的TTL时间，单位为秒，-1表示永不过期。
     * 当前在开启时间线元数据TTL功能时，需要将allowUpdateAttributes配置为false，此时服务端将不再允许更新时间线的元数据属性。
     */
    private OptionalValue<Integer> metaTimeToLive = new OptionalValue<Integer>("MetaTimeToLive");

    /**
     * 是否允许更新时间线元数据属性，当开启元数据TTL功能时需要设置为false。
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
