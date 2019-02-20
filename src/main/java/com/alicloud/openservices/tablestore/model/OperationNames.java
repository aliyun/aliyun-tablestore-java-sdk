package com.alicloud.openservices.tablestore.model;

public class OperationNames {
    public static final String OP_CREATE_TABLE = "CreateTable";

    public static final String OP_LIST_TABLE = "ListTable";

    public static final String OP_DELETE_TABLE = "DeleteTable";

    public static final String OP_DESCRIBE_TABLE = "DescribeTable";

    public static final String OP_UPDATE_TABLE = "UpdateTable";

    public static final String OP_GET_ROW = "GetRow";

    public static final String OP_PUT_ROW = "PutRow";

    public static final String OP_UPDATE_ROW = "UpdateRow";

    public static final String OP_DELETE_ROW = "DeleteRow";

    public static final String OP_BATCH_GET_ROW = "BatchGetRow";

    public static final String OP_BATCH_WRITE_ROW = "BatchWriteRow";

    public static final String OP_GET_RANGE = "GetRange";

    public static final String OP_LIST_STREAM = "ListStream";

    public static final String OP_DESCRIBE_STREAM = "DescribeStream";

    public static final String OP_GET_SHARD_ITERATOR = "GetShardIterator";

    public static final String OP_GET_STREAM_RECORD = "GetStreamRecord";
    
    public static final String OP_COMPUTE_SPLITS_BY_SIZE = "ComputeSplitPointsBySize";

    public static final String OP_START_LOCAL_TRANSACTION = "StartLocalTransaction";

    public static final String OP_COMMIT_TRANSACTION = "CommitTransaction";

    public static final String OP_ABORT_TRANSACTION = "AbortTransaction";

    public static final String OP_CREATE_INDEX = "CreateIndex";

    public static final String OP_DELETE_INDEX = "DropIndex";

    public static final String OP_CREATE_SEARCH_INDEX = "CreateSearchIndex";

    public static final String OP_DELETE_SEARCH_INDEX = "DeleteSearchIndex";

    public static final String OP_LIST_SEARCH_INDEX = "ListSearchIndex";

    public static final String OP_DESCRIBE_SEARCH_INDEX = "DescribeSearchIndex";

    public static final String OP_SEARCH = "Search";

    public static final String OP_CREATE_TUNNEL = "tunnel/create";

    public static final String OP_DELETE_TUNNEL = "tunnel/delete";

    public static final String OP_LIST_TUNNEL = "tunnel/list";

    public static final String OP_DESCRIBE_TUNNEL = "tunnel/describe";

    public static final String OP_CONNECT_TUNNEL = "tunnel/connect";

    public static final String OP_HEARTBEAT = "tunnel/heartbeat";

    public static final String OP_SHUTDOWN_TUNNEL = "tunnel/shutdown";

    public static final String OP_GETCHECKPOINT = "tunnel/getcheckpoint";

    public static final String OP_READRECORDS = "tunnel/readrecords";

    public static final String OP_CHECKPOINT = "tunnel/checkpoint";
}
