package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

/**
 * 
 * ComputeSplitsBySize接口根据用户制定的数据大小进行分块后的数据块类，每个split类表示一串连续的主键值行，每个主键行的遵守目标表格的结构设计。
 * 每个数据块保证其中所包含的数据约等于用户指定的大小。
 */
public class Split implements Jsonizable {
    
    /**
     * 数据分块所在的partition分区的ID的哈希值。
     * 
     */
    private String location;
    
    /**
     * 数据块所包含的起始行的主键值，遵循左闭右开区间。
     * 
     */
    private PrimaryKey lowerBound;
    
    /**
     * 数据块所包含的末尾行的下一行的主键值，遵循左闭右开区间。
     * 
     */
    private PrimaryKey upperBound;

    public Split() {
    }

    public Split(String location, PrimaryKey lowerBound, PrimaryKey upperBound) {
        this.location = location;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public PrimaryKey getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(PrimaryKey lowerBound) {
        this.lowerBound = lowerBound;
    }

    public PrimaryKey getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(PrimaryKey upperBound) {
        this.upperBound = upperBound;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append("{\"Location\": " + this.getLocation() + ", ");
        sb.append("\"lowerBound\": ");
        if (this.getLowerBound() != null) {
            sb.append(this.getLowerBound().jsonize() + " ");
        } else {
            sb.append("null");
        }
        sb.append(", ");
        sb.append("\"upperBound\": ");
        if (this.getUpperBound() != null) {
            sb.append(this.getUpperBound().jsonize() + " ");
        } else {
            sb.append("null");
        }
        sb.append("}");
    }
}
