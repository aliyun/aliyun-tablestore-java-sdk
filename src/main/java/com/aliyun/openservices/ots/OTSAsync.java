/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.model.*;

/**
 * 阿里云开放结构化数据服务（Open Table Service, OTS）的访问接口。
 * <p>
 * 开放结构化数据服务（Open Table Service，OTS）是构建在阿里云大规模分布式计算系统之上 的海量数据存储与实时查询的服务。
 * </p>
 */
public interface OTSAsync {

    /**
     * 创建表（Table）。
     * 
     * @param createTableRequest
     *            执行CreateTable操作所需参数的封装。
     * @return OTSFuture 
     *            createTableResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<CreateTableResult> createTable(
            CreateTableRequest createTableRequest) throws ClientException;

    /**
     * 创建表（Table）。
     * 
     * @param createTableRequest
     *            执行CreateTable操作所需参数的封装。
     * @param callback
     *            执行CreataTable操作完成后的回调函数。
     * @return otsFuture 
     *            createTableResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<CreateTableResult> createTable(
            CreateTableRequest createTableRequest,
            OTSCallback<CreateTableRequest, CreateTableResult> callback)
            throws ClientException;

    /**
     * 返回表（Table）的结构信息。
     * 
     * @param describeTableRequest
     *            执行DescribeTable操作所需参数的封装。
     * @return otsFuture 
     *            describeTableResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<DescribeTableResult> describeTable(
            DescribeTableRequest describeTableRequest) throws ClientException;

    /**
     * 返回表（Table）的结构信息。
     * 
     * @param describeTableRequest
     *            执行DescribeTable操作所需参数的封装。
     * @param callback
     *            执行DescribeTable操作完成后的回调函数。
     * @return otsFuture 
     *            describeTableResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<DescribeTableResult> describeTable(
            DescribeTableRequest describeTableRequest,
            OTSCallback<DescribeTableRequest, DescribeTableResult> callback)
            throws ClientException;

    /**
     * 返回表（Table）名的列表。
     * 
     * @return otsFuture 
     *            listTableResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<ListTableResult> listTable() throws ClientException;

    /**
     * 返回表（Table）名的列表。
     * 
     * @param callback
     *            执行ListTable操作完成后的回调函数。
     * @return otsFuture 
     *            listTableResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<ListTableResult> listTable(
            OTSCallback<ListTableRequest, ListTableResult> callback)
            throws ClientException;

    /**
     * 删除表（Table）。
     * 
     * @param deleteTableRequest
     *            执行DeleteTable操作所需参数的封装。
     * @return otsFuture 
     *            deleteTableResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<DeleteTableResult> deleteTable(
            DeleteTableRequest deleteTableRequest) throws ClientException;

    /**
     * 删除表（Table）。
     * 
     * @param deleteTableRequest
     *            执行DeleteTable操作所需参数的封装。
     * @param callback
     *            执行DeleteTable操作完成后的回调函数。
     * @return otsFuture 
     *            deleteTableResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<DeleteTableResult> deleteTable(
            DeleteTableRequest deleteTableRequest,
            OTSCallback<DeleteTableRequest, DeleteTableResult> callback)
            throws ClientException;

    /**
     * 更新表的读或写CapacityUnit。
     * 
     * @param updateTableRequest
     *            执行UpdateTable操作所需参数的封装。
     * @return otsFuture 
     *            updateTableResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<UpdateTableResult> updateTable(
            UpdateTableRequest updateTableRequest) throws ClientException;

    /**
     * 更新表的读或写CapacityUnit。
     * 
     * @param updateTableRequest
     *            执行UpdateTable操作所需参数的封装
     * @param callback
     *            执行UpdateTable操作完成后的回调函数。
     * @return otsFuture 
     *            updateTableResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<UpdateTableResult> updateTable(
            UpdateTableRequest updateTableRequest,
            OTSCallback<UpdateTableRequest, UpdateTableResult> callback)
            throws ClientException;

    /**
     * 返回表（Table）中的一行数据。
     * 
     * @param getRowRequest
     *            执行GetRow操作所需参数的封装。
     * @return otsFuture 
     *            getRowResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<GetRowResult> getRow(GetRowRequest getRowRequest)
            throws ClientException;

    /**
     * 返回表（Table）中的一行数据。
     * 
     * @param getRowRequest
     *            执行GetRow操作所需参数的封装。
     * @param callback
     *            执行GetRow操作完成后的回调函数。
     * @return otsFuture 
     *            getRowResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<GetRowResult> getRow(GetRowRequest getRowRequest,
            OTSCallback<GetRowRequest, GetRowResult> callback)
            throws ClientException;

    /**
     * 向表（Table）中插入或覆盖一行数据。
     * 
     * @param putRowRequest
     *            执行PutRow操作所需参数的封装。
     * @return otsFuture 
     *            putRowResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<PutRowResult> putRow(PutRowRequest putRowRequest)
            throws ClientException;

    /**
     * 向表（Table）中插入或覆盖一行数据。
     * 
     * @param putRowRequest
     *            执行PutRow操作所需参数的封装。
     * @param callback
     *            执行PutRow操作完成后的回调函数。
     * @return otsFuture 
     *            putRowResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<PutRowResult> putRow(PutRowRequest putRowRequest,
            OTSCallback<PutRowRequest, PutRowResult> callback)
            throws ClientException;

    /**
     * 更改表（Table）中的一行数据。
     * 
     * @param updateRowRequest
     *            执行UpdateRow操作所需参数的封装。
     * @return otsFuture 
     *            updateRowResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<UpdateRowResult> updateRow(
            UpdateRowRequest updateRowRequest) throws ClientException;

    /**
     * 更改表（Table）中的一行数据。
     * 
     * @param updateRowRequest
     *            执行UpdateRow操作所需参数的封装。
     * @param callback
     *            执行UpdateRow操作完成后的回调函数。
     * @return otsFuture 
     *            updateRowResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<UpdateRowResult> updateRow(
            UpdateRowRequest updateRowRequest,
            OTSCallback<UpdateRowRequest, UpdateRowResult> callback)
            throws ClientException;

    /**
     * 删除表（Table）中的一行数据。
     * 
     * @param deleteRowRequest
     *            执行DeleteRow操作所需参数的封装。
     * @return otsFuture 
     *            updateRowResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<DeleteRowResult> deleteRow(
            DeleteRowRequest deleteRowRequest) throws ClientException;

    /**
     * 删除表（Table）中的一行数据。
     * 
     * @param deleteRowRequest
     *            执行DeleteRow操作所需参数的封装。
     * @param callback
     *            执行DeleteRow操作完成后的回调函数。
     * @return otsFuture 
     *            updateRowResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<DeleteRowResult> deleteRow(
            DeleteRowRequest deleteRowRequest,
            OTSCallback<DeleteRowRequest, DeleteRowResult> callback)
            throws ClientException;

    /**
     * 从多张表（Table）中获取多行数据。
     * 
     * @param batchGetRowRequest
     *            执行BatchGetRow操作所需参数的封装。
     * @return otsFuture 
     *            batchGetRowResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<BatchGetRowResult> batchGetRow(
            BatchGetRowRequest batchGetRowRequest) throws ClientException;

    /**
     * 从多张表（Table）中获取多行数据。
     * 
     * @param batchGetRowRequest
     *            执行BatchGetRow操作所需参数的封装。
     * @param callback
     *            执行BatchGetRow操作完成后的回调函数。
     * @return otsFuture 
     *            batchGetRowResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<BatchGetRowResult> batchGetRow(
            BatchGetRowRequest batchGetRowRequest,
            OTSCallback<BatchGetRowRequest, BatchGetRowResult> callback)
            throws ClientException;

    /**
     * 对多张表（Table）中的多行数据进行增加、删除或者更改操作。
     * 
     * @param batchWriteRowRequest
     *            执行BatchWriteRow操作所需参数的封装。
     * @return otsFuture 
     *            batchWriteRowResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<BatchWriteRowResult> batchWriteRow(
            BatchWriteRowRequest batchWriteRowRequest) throws ClientException;

    /**
     * 对多张表（Table）中的多行数据进行增加、删除或者更改操作。
     * 
     * @param batchWriteRowRequest
     *            执行BatchWriteRow操作所需参数的封装。
     * @param callback
     *            执行BatchWriteRow操作完成后的回调函数。
     * @return otsFuture 
     *            batchWriteRowResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<BatchWriteRowResult> batchWriteRow(
            BatchWriteRowRequest batchWriteRowRequest,
            OTSCallback<BatchWriteRowRequest, BatchWriteRowResult> callback)
            throws ClientException;

    /**
     * 从表（Table）中查询一个范围内的多行数据。
     * 
     * @param getRangeRequest
     *            执行GetRange操作所需参数的封装。
     * @return otsFuture 
     *            getRangeResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<GetRangeResult> getRange(GetRangeRequest getRangeRequest)
            throws ClientException;

    /**
     * 从表（Table）中查询一个范围内的多行数据。
     * 
     * @param getRangeRequest
     *            执行GetRange操作所需参数的封装。
     * @param callback
     *            执行GetRange操作完成后的回调函数。
     * @return otsFuture 
     *            getRangeResult通过otsFuture的get方法返回。
     * @throws ClientException
     *            请求的返回结果无效， 或由于网络原因请求失败， 或访问超时。
     */
    public OTSFuture<GetRangeResult> getRange(GetRangeRequest getRangeRequest,
            OTSCallback<GetRangeRequest, GetRangeResult> callback)
            throws ClientException;

    /**
     * 释放资源。
     *
     * <p>
     * 请确保在所有请求执行完毕之后释放资源。释放资源之后将不能再发送请求，正在执行的请求可能无法返回结果。
     * </p>
     */
    public void shutdown();
}