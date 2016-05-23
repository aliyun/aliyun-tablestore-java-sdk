package com.aliyun.openservices.ots.parser;

import com.aliyun.openservices.ots.comm.ResponseMessage;

/**
 * Used to convert an result stream to a java object.
 *
 */
public interface ResultParser {
    /**
     * Converts the result from stream to a java object.
     * @param response The stream of the result.
     * @return The java object that the result stands for.
     * @throws ResultParseException Failed to parse the result.
     */
    public Object getObject(ResponseMessage response) throws ResultParseException;
}