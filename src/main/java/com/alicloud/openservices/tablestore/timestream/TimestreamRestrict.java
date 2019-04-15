package com.alicloud.openservices.tablestore.timestream;

/**
 * Timestream API限制项
 */
public class TimestreamRestrict {
    /**
     * {@link com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier}的name的长度限制
     */
    public final static int NAME_LEN_BYTE = 100;
    /**
     * 每个{@link com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier}的tag的个数限制
     */
    public final static int TAG_COUNT = 12;
    /**
     * 一个{@link com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier}的所有tag的name和value的长度总和上限
     */
    public final static int TAG_LEN_BYTE = 500;
    /**
     * 每个{@link com.alicloud.openservices.tablestore.timestream.model.TimestreamMeta}的attribute的个数限制
     */
    public final static int ATTR_COUNT = 20;
    /**
     * 一个{@link com.alicloud.openservices.tablestore.timestream.model.TimestreamMeta}的所有attribute的name和value的长度总和上限
     */
    public final static int ATTR_LEN_BYTE = 4000;
}
