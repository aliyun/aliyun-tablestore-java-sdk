/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.parser;

import com.aliyun.openservices.ots.comm.ResponseMessage;

/**
 * Used to convert an result stream to a java object.
 *
 */
public interface ResultParser {
    /**
     * Converts the result from stream to a java object.
     * @param resultStream The stream of the result.
     * @return The java object that the result stands for.
     * @throws ResultParseException Failed to parse the result.
     */
    public Object getObject(ResponseMessage response) throws ResultParseException;
}