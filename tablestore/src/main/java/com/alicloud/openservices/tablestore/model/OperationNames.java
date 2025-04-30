package com.alicloud.openservices.tablestore.model;

import java.util.HashMap;
import java.util.Map;

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

    public static final String OP_BULK_IMPORT = "BulkImport";

    public static final String OP_GET_RANGE = "GetRange";

    public static final String OP_BULK_EXPORT = "BulkExport";

    public static final String OP_LIST_STREAM = "ListStream";

    public static final String OP_DESCRIBE_STREAM = "DescribeStream";

    public static final String OP_GET_SHARD_ITERATOR = "GetShardIterator";

    public static final String OP_GET_STREAM_RECORD = "GetStreamRecord";
    
    public static final String OP_COMPUTE_SPLITS_BY_SIZE = "ComputeSplitPointsBySize";

    public static final String OP_CREATE_DELIVERY_TASK = "CreateDeliveryTask";

    public static final String OP_DELETE_DELIVERY_TASK = "DeleteDeliveryTask";

    public static final String OP_UPDATE_DELIVERY_TASK = "UpdateDeliveryTask";

    public static final String OP_DESCRIBE_DELIVERY_TASK = "DescribeDeliveryTask";

    public static final String OP_LIST_DELIVERY_TASK = "ListDeliveryTask";

    public static final String OP_START_LOCAL_TRANSACTION = "StartLocalTransaction";

    public static final String OP_COMMIT_TRANSACTION = "CommitTransaction";

    public static final String OP_ABORT_TRANSACTION = "AbortTransaction";

    public static final String OP_CREATE_INDEX = "CreateIndex";

    public static final String OP_DELETE_INDEX = "DropIndex";

    public static final String OP_ADD_DEFINED_COLUMN = "AddDefinedColumn";

    public static final String OP_DELETE_DEFINED_COLUMN = "DeleteDefinedColumn";

    public static final String OP_CREATE_SEARCH_INDEX = "CreateSearchIndex";

    public static final String OP_UPDATE_SEARCH_INDEX = "UpdateSearchIndex";

    public static final String OP_DELETE_SEARCH_INDEX = "DeleteSearchIndex";

    public static final String OP_LIST_SEARCH_INDEX = "ListSearchIndex";

    public static final String OP_DESCRIBE_SEARCH_INDEX = "DescribeSearchIndex";

    public static final String OP_SEARCH = "Search";

    public static final String OP_PARALLEL_SCAN = "ParallelScan";

    public static final String OP_COMPUTE_SPLITS = "ComputeSplits";

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

    public static final String OP_PUT_TIMESERIES_DATA = "PutTimeseriesData";

    public static final String OP_GET_TIMESERIES_DATA = "GetTimeseriesData";

    public static final String OP_QUERY_TIMESERIES_META = "QueryTimeseriesMeta";

    public static final String OP_LIST_TIMESERIES_TABLE = "ListTimeseriesTable";

    public static final String OP_CREATE_TIMESERIES_TABLE = "CreateTimeseriesTable";

    public static final String OP_DELETE_TIMESERIES_TABLE = "DeleteTimeseriesTable";

    public static final String OP_DESCRIBE_TIMESERIES_TABLE = "DescribeTimeseriesTable";

    public static final String OP_UPDATE_TIMESERIES_TABLE = "UpdateTimeseriesTable";

    public static final String OP_UPDATE_TIMESERIES_META = "UpdateTimeseriesMeta";

    public static final String OP_DELETE_TIMESERIES_META = "DeleteTimeseriesMeta";

    public static final String OP_SPLIT_TIMESERIES_SCAN_TASK = "SplitTimeseriesScanTask";

    public static final String OP_SCAN_TIMESERIES_DATA = "ScanTimeseriesData";

    public static final String OP_CREATE_TIMESERIES_ANALYTICAL_STORE = "CreateTimeseriesAnalyticalStore";

    public static final String OP_UPDATE_TIMESERIES_ANALYTICAL_STORE = "UpdateTimeseriesAnalyticalStore";

    public static final String OP_DELETE_TIMESERIES_ANALYTICAL_STORE = "DeleteTimeseriesAnalyticalStore";

    public static final String OP_DESCRIBE_TIMESERIES_ANALYTICAL_STORE = "DescribeTimeseriesAnalyticalStore";

    public static final String OP_CREATE_TIMESERIES_LASTPOINT_INDEX = "CreateTimeseriesLastpointIndex";

    public static final String OP_DELETE_TIMESERIES_LASTPOINT_INDEX = "DeleteTimeseriesLastpointIndex";

    public static final String OP_SQL_Query = "SQLQuery";

    public static class IdempotentActionTool {
        private static final Map<String, Boolean> IDEMPOTENT_ACTIONS = new HashMap<String, Boolean>();

        static {
            // table operations
            IDEMPOTENT_ACTIONS.put(OP_LIST_TABLE, true);
            IDEMPOTENT_ACTIONS.put(OP_DESCRIBE_TABLE, true);
            // row operations
            IDEMPOTENT_ACTIONS.put(OP_GET_ROW, true);
            IDEMPOTENT_ACTIONS.put(OP_BATCH_GET_ROW, true);
            IDEMPOTENT_ACTIONS.put(OP_GET_RANGE, true);
            IDEMPOTENT_ACTIONS.put(OP_BULK_EXPORT, true);
            // stream operations
            IDEMPOTENT_ACTIONS.put(OP_LIST_STREAM, true);
            IDEMPOTENT_ACTIONS.put(OP_DESCRIBE_STREAM, true);
            IDEMPOTENT_ACTIONS.put(OP_GET_SHARD_ITERATOR, true);
            IDEMPOTENT_ACTIONS.put(OP_GET_STREAM_RECORD, true);
            IDEMPOTENT_ACTIONS.put(OP_COMPUTE_SPLITS_BY_SIZE, true);
            // deliveryTask operations
            IDEMPOTENT_ACTIONS.put(OP_DESCRIBE_DELIVERY_TASK, true);
            IDEMPOTENT_ACTIONS.put(OP_LIST_DELIVERY_TASK, true);
            // searchIndex operations
            IDEMPOTENT_ACTIONS.put(OP_LIST_SEARCH_INDEX, true);
            IDEMPOTENT_ACTIONS.put(OP_DESCRIBE_SEARCH_INDEX, true);
            IDEMPOTENT_ACTIONS.put(OP_SEARCH, true);
            // timeseries operations
            IDEMPOTENT_ACTIONS.put(OP_GET_TIMESERIES_DATA, true);
            IDEMPOTENT_ACTIONS.put(OP_QUERY_TIMESERIES_META, true);
            IDEMPOTENT_ACTIONS.put(OP_LIST_TIMESERIES_TABLE, true);
            IDEMPOTENT_ACTIONS.put(OP_DESCRIBE_TIMESERIES_TABLE, true);
            IDEMPOTENT_ACTIONS.put(OP_SCAN_TIMESERIES_DATA, true);
            IDEMPOTENT_ACTIONS.put(OP_DESCRIBE_TIMESERIES_ANALYTICAL_STORE, true);
            // deliveryTask operations
            IDEMPOTENT_ACTIONS.put(OP_PARALLEL_SCAN, true);
            IDEMPOTENT_ACTIONS.put(OP_COMPUTE_SPLITS, true);
            // tunnel operations
            IDEMPOTENT_ACTIONS.put(OP_LIST_TUNNEL, true);
            IDEMPOTENT_ACTIONS.put(OP_DESCRIBE_TUNNEL, true);
            IDEMPOTENT_ACTIONS.put(OP_READRECORDS, true);
            IDEMPOTENT_ACTIONS.put(OP_GETCHECKPOINT, true);
        }

        public static boolean isIdempotentAction(String action) {
            /**
             * all read operations are idempotent
             */
            Boolean isIdempotent = IDEMPOTENT_ACTIONS.get(action);
            return isIdempotent != null && isIdempotent;
        }
    }
}
