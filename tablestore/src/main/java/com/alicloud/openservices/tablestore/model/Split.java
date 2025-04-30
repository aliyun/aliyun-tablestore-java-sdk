package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

/**
 * 
 * The ComputeSplitsBySize interface is a class for data blocks that are split based on the data size specified by the user. Each split represents a sequence of consecutive primary key value rows, and each primary key row complies with the structural design of the target table.
 * Each data block ensures that the amount of data it contains is approximately equal to the size specified by the user.
 */
public class Split implements Jsonizable {
    
    /**
     * The hash value of the ID of the partition where the data chunk is located.
     * 
     */
    private String location;
    
    /**
     * The primary key value of the starting row contained in the data block, follows the left-closed-right-open interval.
     */
    private PrimaryKey lowerBound;
    
    /**
     * The primary key value of the next row after the last row included in the data block, following the left-closed-right-open interval.
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
