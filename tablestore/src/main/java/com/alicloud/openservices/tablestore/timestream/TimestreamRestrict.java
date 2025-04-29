package com.alicloud.openservices.tablestore.timestream;

/**
 * Timestream API limitations
 */
public class TimestreamRestrict {
    /**
     * The length limit of the name in {@link com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier}
     */
    public final static int NAME_LEN_BYTE = 100;
    /**
     * The number of tags for each {@link com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier} is limited.
     */
    public final static int TAG_COUNT = 12;
    /**
     * The upper limit of the total length of all names and values of tags in a {@link com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier}.
     */
    public final static int TAG_LEN_BYTE = 500;
    /**
     * The limit on the number of attributes for each {@link com.alicloud.openservices.tablestore.timestream.model.TimestreamMeta}.
     */
    public final static int ATTR_COUNT = 20;
    /**
     * The upper limit of the total sum of lengths of all attribute names and values in a {@link com.alicloud.openservices.tablestore.timestream.model.TimestreamMeta}.
     */
    public final static int ATTR_LEN_BYTE = 4000;
}
