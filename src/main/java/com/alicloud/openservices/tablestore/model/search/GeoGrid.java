package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class GeoGrid implements Jsonizable {
    private GeoPoint topLeft;
    private GeoPoint bottomRight;

    public GeoGrid() {
    }

    public GeoGrid(GeoPoint topLeft, GeoPoint bottomRight) {
        this.topLeft = topLeft;
        this.bottomRight = bottomRight;
    }

    public GeoPoint getTopLeft() {
        return topLeft;
    }

    public void setTopLeft(GeoPoint topLeft) {
        this.topLeft = topLeft;
    }

    public GeoPoint getBottomRight() {
        return bottomRight;
    }

    public void setBottomRight(GeoPoint bottomRight) {
        this.bottomRight = bottomRight;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{').append(newline);
        sb.append("  ").append("\"topLeft\": ");
        if (topLeft != null) {
            topLeft.jsonize(sb, newline + "  ");
        } else {
            sb.append("null");
        }
        sb.append(',').append(newline);
        sb.append("  ").append("\"bottomRight\": ");
        if (bottomRight != null) {
            bottomRight.jsonize(sb, newline + "  ");
        } else {
            sb.append("null");
        }
        sb.append(newline).append('}');
    }
}
