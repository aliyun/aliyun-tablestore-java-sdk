package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.*;

import java.io.IOException;

/**
 * In TableStore, each row of data contains a primary key ({@link PrimaryKey}),
 * which is composed of multiple primary key columns ({@link PrimaryKeyColumn}),
 * and each primary key column includes the name and value of the primary key column ({@link PrimaryKeyValue}).
 */
public class PrimaryKeyColumn implements Comparable<PrimaryKeyColumn>, Jsonizable, Measurable {
    /**
     * The name of the primary key column.
     */
    private String name;

    /**
     * The value of the primary key column.
     */
    private PrimaryKeyValue value;

    /**
     * The data size occupied after serialization
     */
    private int dataSize = -1;

    /**
     * Constructs the primary key column based on the specified name and value of the primary key column.
     * <p>The name of the primary key column cannot be a null pointer or an empty string.</p>
     * <p>The value of the primary key column cannot be a null pointer.</p>
     *
     * @param name  The name of the primary key column
     * @param value The value of the primary key column
     */
    public PrimaryKeyColumn(String name, PrimaryKeyValue value) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of primary key should not be null or empty.");
        Preconditions.checkNotNull(value, "The value of primary key should not be null.");

        this.name = name;
        this.value = value;
    }

    /**
     * Get the name of the primary key column.
     *
     * @return the name of the primary key column
     */
    public String getName() {
        return name;
    }

    public byte[] getNameRawData() {
        return Bytes.toBytes(name);
    }

    /**
     * Get the value of the primary key column.
     *
     * @return the value of the primary key column
     */
    public PrimaryKeyValue getValue() {
        return value;
    }

    /**
     * Convert the primary key column type to the attribute column type.
     * @return
     */
    public Column toColumn() throws IOException {
        return new Column(getName(), getValue().toColumnValue());
    }

    @Override
    public String toString() {
        return "'" + name + "':" + value;
    }

    @Override
    public int hashCode() {
        return name.hashCode() ^ value.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof PrimaryKeyColumn)) {
            return false;
        }

        PrimaryKeyColumn col = (PrimaryKeyColumn) o;
        return this.name.equals(col.name) && this.value.equals(col.value);
    }

    /**
     * Compare the size of two primary key columns.
     * <p>The two primary key columns being compared must have the same name and type.</p>
     *
     * @param target
     * @return Returns 0 if they are equal, 1 if greater than, and -1 if less than.
     */
    @Override
    public int compareTo(PrimaryKeyColumn target) {
        if (!this.name.equals(target.name)) {
            throw new IllegalArgumentException("The name of primary key to be compared must be the same.");
        }

        return this.value.compareTo(target.value);
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append("{\"Name\": \"");
        sb.append(name);
        sb.append("\", \"Type\": \"");
        if ( value.getType() != null ) {
            sb.append(value.getType().toString());
            sb.append("\", \"Value\": ");
            switch(value.getType()) {
            case INTEGER:
                sb.append(value.asLong());
                break;
            case STRING:
                sb.append("\"");
                sb.append(value.asString());
                sb.append("\"");
                break;
            case BINARY:
                sb.append("\"");
                sb.append(Base64.toBase64String(value.asBinary()));
                sb.append("\"");
                break;
            default:
                throw new IllegalArgumentException("Unknown type: " + value.getType());
            }
        } else {
            sb.append("null");
            sb.append("\", \"Value\": ");
            if ( value.isInfMin() ) {
                sb.append("\"");
                sb.append("INF_MIN");
                sb.append("\"");
            } else if ( value.isInfMax() ) {
                sb.append("\"");
                sb.append("INF_MAX");
                sb.append("\"");
            } else {
                throw new IllegalArgumentException("Unknown value: " + value.asString());
            }
        }
        sb.append("}");
    }

    @Override
    public int getDataSize() {
        if (dataSize == -1) {
            dataSize = CalculateHelper.calcStringSizeInBytes(name) + value.getDataSize();
        }

        return dataSize;
    }

}
