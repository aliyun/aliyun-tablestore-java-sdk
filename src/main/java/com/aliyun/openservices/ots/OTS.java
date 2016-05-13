/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots;

import java.util.Iterator;

import com.aliyun.openservices.ots.model.*;

/**
 * 阿里云开放结构化数据服务（Open Table Service, OTS）的访问接口。
 * <p>
 * 开放结构化数据服务（Open Table Service，OTS）是构建在阿里云大规模分布式计算系统之上 的海量数据存储与实时查询的服务。
 * </p>
 */
public interface OTS {

    /**
     * 创建表（Table）。
     * 
     * @param createTableRequest
     *            执行CreateTable操作所需参数的封装。
     * @return CreateTable操作的响应内容。
     * @throws OTSException
     *             OTS访问返回错误消息
     * @throws ClientException
     *             请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public CreateTableResult createTable(CreateTableRequest createTableRequest)
            throws OTSException, ClientException;

    /**
     * 返回表（Table）的结构信息。
     * 
     * @param describeTableRequest
     *            执行DescribeTable操作所需参数的封装。
     * @return DescribeTable操作的响应内容。
     * @throws OTSException
     *             OTS访问返回错误消息
     * @throws ClientException
     *             请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public DescribeTableResult describeTable(
            DescribeTableRequest describeTableRequest) throws OTSException,
            ClientException;

    /**
     * 返回表（Table）名的列表。
     * 
     * @return ListTable操作的响应内容。
     * @throws OTSException
     *             OTS访问返回错误消息
     * @throws ClientException
     *             请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public ListTableResult listTable() throws OTSException, ClientException;

    /**
     * 删除表（Table）。
     * 
     * @param deleteTableRequest
     *            执行DeleteTable操作所需参数的封装。
     * @return DeleteTable操作的响应内容。
     * @throws OTSException
     *             OTS访问返回错误消息
     * @throws ClientException
     *             请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest)
            throws OTSException, ClientException;

    /**
     * 更新表的读或写CapacityUnit。
     * 
     * @param updateTableRequest
     *            执行UpdateTable操作所需参数的封装。
     * @return UpdateTable操作的响应内容。
     * @throws OTSException
     *             OTS访问返回错误消息
     * @throws ClientException
     *             请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest)
            throws OTSException, ClientException;

    /**
     * 返回表（Table）中的一行数据。
     * 
     * @param getRowRequest
     *            执行GetRow操作所需参数的封装。
     * @return GetRow操作的响应内容。
     * @throws OTSException
     *             OTS访问返回错误消息
     * @throws ClientException
     *             请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public GetRowResult getRow(GetRowRequest getRowRequest)
            throws OTSException, ClientException;

    /**
     * 向表（Table）中插入或覆盖一行数据。
     * 
     * @param putRowRequest
     *            执行PutRow操作所需参数的封装。
     * @return PutRow操作的响应内容。
     * @throws OTSException
     *             OTS访问返回错误消息
     * @throws ClientException
     *             请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public PutRowResult putRow(PutRowRequest putRowRequest)
            throws OTSException, ClientException;

    /**
     * 更改表（Table）中的一行数据。
     * 
     * @param updateRowRequest
     *            执行UpdateRow操作所需参数的封装。
     * @return UpdateRow操作的响应内容。
     * @throws OTSException
     *             OTS访问返回错误消息
     * @throws ClientException
     *             请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public UpdateRowResult updateRow(UpdateRowRequest updateRowRequest)
            throws OTSException, ClientException;

    /**
     * 删除表（Table）中的一行数据。
     * 
     * @param deleteRowRequest
     *            执行DeleteRow操作所需参数的封装。
     * @return DeleteRow操作的响应内容。
     * @throws OTSException
     *             OTS访问返回错误消息
     * @throws ClientException
     *             请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public DeleteRowResult deleteRow(DeleteRowRequest deleteRowRequest)
            throws OTSException, ClientException;

    /**
     * 从多张表（Table）中获取多行数据。
     * 
     * @param batchGetRowRequest
     *            执行BatchGetRow操作所需参数的封装。
     * @return BatchGetRow操作的响应内容。
     * @throws OTSException
     *             OTS访问返回错误消息
     * @throws ClientException
     *             请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public BatchGetRowResult batchGetRow(BatchGetRowRequest batchGetRowRequest)
            throws OTSException, ClientException;

    /**
     * 对多张表（Table）中的多行数据进行增加、删除或者更改操作。
     * 
     * @param batchWriteRowRequest
     *            执行BatchWriteRow操作所需参数的封装。
     * @return BatchWriteRow操作的响应内容。
     * @throws OTSException
     *             OTS访问返回错误消息
     * @throws ClientException
     *             请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public BatchWriteRowResult batchWriteRow(
            BatchWriteRowRequest batchWriteRowRequest) throws OTSException,
            ClientException;

    /**
     * 从表（Table）中查询一个范围内的多行数据。
     * 
     * @param getRangeRequest
     *            执行GetRange操作所需参数的封装。
     * @return GetRange操作的响应内容。
     * @throws OTSException
     *             OTS访问返回错误消息
     * @throws ClientException
     *             请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public GetRangeResult getRange(GetRangeRequest getRangeRequest)
            throws OTSException, ClientException;

    /**
     *  从表(Table)中迭代读取满足条件的数据
     * @param rangeIteratorParameter
     *         迭代读时的参数，包括开始，结束位置
     * @return 迭代器
     * @throws OTSException
     *          OTS访问返回错误消息
     * @throws ClientException
     *          请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public Iterator<Row> createRangeIterator(
            RangeIteratorParameter rangeIteratorParameter)
            throws OTSException, ClientException;

    /**
     * 释放资源。
     * 
     * <p>
     * 请确保在所有请求执行完毕之后释放资源。释放资源之后将不能再发送请求，正在执行的请求可能无法返回结果。
     * </p>
     */
    public void shutdown();
}