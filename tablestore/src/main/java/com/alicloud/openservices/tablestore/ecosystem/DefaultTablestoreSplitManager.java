package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * generate splits by filter
 */
public class DefaultTablestoreSplitManager implements ITablestoreSplitManager {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultTablestoreSplitManager.class);

    private ICatalogManager manager;

    public DefaultTablestoreSplitManager(SyncClient client) {
        manager = new CatalogManager(client);
    }


    private final static AtomicBoolean enableSplitCache = new AtomicBoolean(true);
    // key: instance_table_splitPointLimit_splitSizeInMBs
    private static final Map<String, ComputeSplitsBySizeResponse> splitsCache =
            new ConcurrentHashMap<String, ComputeSplitsBySizeResponse>();

    public static void setEnableSplitCache(boolean enableSplitCache) {
        DefaultTablestoreSplitManager.enableSplitCache.set(enableSplitCache);
    }

    @Override
    public List<ITablestoreSplit> generateTablestoreSplits(
            SyncClient client,
            Filter filter,
            String tableName,
            ComputeParameters parameter,
            List<String> requiredColumns) {
        TableCatalog catalog;
        catalog = manager.getTableCatalog(tableName);
        if (catalog == null) {
            throw new InvalidParameterException("table does not exist");
        }
        List<ITablestoreSplit> tableSplits = new ArrayList<ITablestoreSplit>();
        LeftMatchResult calculateResult = new LeftMatchResult(tableName, catalog.getTableMeta());
        if (parameter.getComputeMode() == ComputeParameters.ComputeMode.Auto) {
            calculateResult = calculateComputeMode(client, catalog, filter, parameter, requiredColumns);
        }

        if (parameter.getComputeMode() == ComputeParameters.ComputeMode.KV) {
            List<Split> rawSplits = getTableSplits(client, calculateResult.getTableName(), parameter);
            for (Split rawSplit : rawSplits) {
                TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.KeyValue, filter, requiredColumns);
                split.setKvSplit(rawSplit);
                split.setKvTableMeta(calculateResult.getTableMeta());
                split.setTableName(tableName);
                split.setSplitName(calculateResult.getTableName());
                if (split.checkIfMatchTheFilter(filter)) {
                    tableSplits.add(split);
                }
            }
        } else if (parameter.getComputeMode() == ComputeParameters.ComputeMode.Search) {
            // generate one split and later use parallel scan
            if (parameter.getSearchIndexName() != null) {
                DescribeSearchIndexRequest request = new DescribeSearchIndexRequest();
                request.setTableName(tableName);
                request.setIndexName(parameter.getSearchIndexName());
                DescribeSearchIndexResponse response = client.describeSearchIndex(request);
                IndexSchema schema = response.getSchema();
                List<String> geoFieldList = new ArrayList<String>();
                for (FieldSchema fieldSchema : schema.getFieldSchemas()) {
                    if (fieldSchema.getFieldType() == FieldType.GEO_POINT) {
                        geoFieldList.add(fieldSchema.getFieldName());
                    }
                }

                ComputeSplitsRequest computeSplitsRequest = new ComputeSplitsRequest();
                computeSplitsRequest.setTableName(tableName);
                computeSplitsRequest.setSplitsOptions(new SearchIndexSplitsOptions(parameter.getSearchIndexName()));
                ComputeSplitsResponse computeSplitsResponse = client.computeSplits(computeSplitsRequest);
                byte[] sessionId = computeSplitsResponse.getSessionId();
                int splitsSize = computeSplitsResponse.getSplitsSize();
                if (parameter.getMaxSplitsCount() > 0 && parameter.getMaxSplitsCount() < splitsSize) {
                    splitsSize = parameter.getMaxSplitsCount();
                }

                for (int i = 0; i < splitsSize; i++) {
                    TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.SearchIndex, filter, requiredColumns, sessionId, i, splitsSize, geoFieldList);
                    SearchInfo info = new SearchInfo(parameter.getSearchIndexName(), catalog.getSearchSchema().get(0));
                    split.setSearchInfo(info);
                    split.setTableName(tableName);
                    split.setSplitName(parameter.getSearchIndexName());
                    tableSplits.add(split);
                }
            } else {
                TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.SearchIndex, filter, requiredColumns);
                SearchInfo info = new SearchInfo(catalog.getSearchNames().get(0), catalog.getSearchSchema().get(0));
                split.setSearchInfo(info);
                split.setTableName(tableName);
                split.setSplitName(catalog.getSearchNames().get(0));
                tableSplits.add(split);
            }
        }
        return tableSplits;
    }

    private String formatSplitsCacheKey(String instanceName, String tableName, ComputeParameters parameter) {
        StringBuilder sb = new StringBuilder();
        sb.append(instanceName).append("_");
        sb.append(tableName).append("_");
        sb.append(parameter.getMaxSplitsCount()).append("_");
        sb.append(parameter.getSplitSizeInMBs());
        return sb.toString();
    }

    public List<Split> getTableSplits(SyncClient client, String tableName, ComputeParameters parameter) {
        String key = formatSplitsCacheKey(client.getInstanceName(), tableName, parameter);
        if (enableSplitCache.get() && splitsCache.containsKey(key)) {
            LOG.info("Load splits from cache, key: {}", key);
            return splitsCache.get(key).getSplits();
        } else {
            ComputeSplitsBySizeRequest request = new ComputeSplitsBySizeRequest();
            request.setTableName(tableName);
            request.setSplitPointLimit(parameter.getMaxSplitsCount());
            request.setSplitSizeInByte(parameter.getSplitSizeInMBs(), 1024 * 1024);
            ComputeSplitsBySizeResponse response = client.computeSplitsBySize(request);
            LOG.info("ComputeSplitsBySize, request param: [{}, {}], resp split size: {}",
                    parameter.getMaxSplitsCount(), parameter.getSplitSizeInMBs(), response.getSplits().size());
            if (enableSplitCache.get()) {
                splitsCache.put(key, response);
            }
            return response.getSplits();
        }
    }

    public LeftMatchResult calculateComputeMode(SyncClient client, TableCatalog catalog, Filter filter, ComputeParameters parameter, List<String> requiredColumns) {
        LeftMatchResult mainTableResult = calculateKvLeftMatchResult(filter, catalog.getTableMeta());
        LeftMatchResult bestTableResult = mainTableResult;
        for (TableMeta meta : catalog.getIndexMetaList()) {
            LeftMatchResult indexResult = calculateKvLeftMatchResult(filter, meta);
            // todo make sure index have all the columns
            if (indexResult.getCount() > bestTableResult.getCount()) {
                bestTableResult = indexResult;
            }

            if (bestTableResult.getCount() == meta.getPrimaryKeyList().size()) {
                break;
            }
        }

        if (catalog.getSearchSchema().size() > 0 && bestTableResult.getCount() == 0 && isSearchIndexMatchTheFilter(catalog.getSearchSchema(), requiredColumns)) {
            // todo : improve searchindex selection after searchindex support parallel scan
            parameter.setComputeMode(ComputeParameters.ComputeMode.Search);
            return new LeftMatchResult(catalog.getSearchNames().get(0), null);
        } else {
            parameter.setComputeMode(ComputeParameters.ComputeMode.KV);
            return bestTableResult;
        }
    }

    private Boolean isSearchIndexMatchTheFilter(List<IndexSchema> indexSchemas, List<String> requiredColumns) {
        boolean indexMatched = true;
        for (IndexSchema schema : indexSchemas) {
            for (String requireColumn : requiredColumns) {
                if (!isSearchIndexMatchTheFilter(requireColumn, schema)) {
                    indexMatched = false;
                    break;
                }
            }
        }
        return indexMatched;
    }

    private boolean isSearchIndexMatchTheFilter(String columnName, IndexSchema schema) {
        for (FieldSchema field : schema.getFieldSchemas()) {
            if (field.getFieldName().equals(columnName) && field.isStore() && field.isIndex()) {
                return true;
            }
        }
        return false;
    }

    private LeftMatchResult calculateKvLeftMatchResult(Filter filter, TableMeta meta) {
        LeftMatchResult result;
        if (filter.isNested()) {
            List<LeftMatchResult> resultCollection = new ArrayList<LeftMatchResult>();
            for (Filter subFilter : filter.getSubFilters()) {
                if (subFilter.isNested()) {
                    LeftMatchResult tempResult = calculateKvLeftMatchResult(subFilter, meta);
                    resultCollection.add(tempResult);
                } else {
                    LeftMatchResult tempResult = buildMatchResult(subFilter, meta);
                    resultCollection.add(tempResult);
                }
            }
            result = buildMatchResult(resultCollection, filter.getLogicOperator());
        } else {
            return buildMatchResult(filter, meta);
        }

        return result;
    }

    private static LeftMatchResult buildMatchResult(Filter filter, TableMeta meta) {
        int index = 0;
        for (PrimaryKeySchema pk : meta.getPrimaryKeyList()) {
            if (filter.getCompareOperator() == Filter.CompareOperator.EMPTY_FILTER) {
                return new LeftMatchResult(meta.getTableName(), meta);
            }
            if (filter.getColumnName().equals(pk.getName()) && isTypeMatched(pk.getType(), filter.getColumnValue().getType())) {
                List<String> temp = new ArrayList<String>();
                temp.add(pk.getName());
                boolean canAppend = false;
                if (filter.getCompareOperator() == Filter.CompareOperator.EQUAL) {
                    canAppend = true;
                }
                LeftMatchResult tempResult = new LeftMatchResult(meta.getTableName(), temp, canAppend, index, meta);
                return tempResult;
            }
            index++;
        }
        return new LeftMatchResult(meta.getTableName(), meta);
    }

    private static boolean isTypeMatched(PrimaryKeyType pkType, ColumnType columnType) {
        return (pkType == PrimaryKeyType.STRING && columnType == ColumnType.STRING
                || pkType == PrimaryKeyType.INTEGER && columnType == ColumnType.INTEGER
                || pkType == PrimaryKeyType.BINARY && columnType == ColumnType.BINARY);
    }

    private LeftMatchResult seekMatchResult(List<LeftMatchResult> collection, int starPos) {
        int maxLength = 0;
        LeftMatchResult longestResult = null;
        for (LeftMatchResult result : collection) {
            if (result.getBeginPkIndex() == starPos) {
                if (result.getCount() > maxLength) {
                    maxLength = result.getCount();
                    longestResult = result;
                } else if (result.getCount() == maxLength && result.getCount() > 0 && result.getCanAppendNewKey()) {
                    maxLength = result.getCount();
                    longestResult = result;
                }
            }
        }
        return longestResult;
    }

    /**
     * for "and" operator we check each sub filter and join them
     * for "or" operator we check each sub filter and choose the min LeftMatchResult
     */
    private LeftMatchResult buildMatchResult(List<LeftMatchResult> collection, Filter.LogicOperator lo) {
        //todo: we may support more LogicOperator
        if (lo == Filter.LogicOperator.OR) {
            LeftMatchResult current = collection.get(0);
            for (LeftMatchResult result : collection) {
                if (result.getCount() < current.getCount()) {
                    current = result;
                }
            }
            return current;
        } else if (lo == Filter.LogicOperator.AND) {
            LeftMatchResult last = collection.get(0);
            int starPos = 0;
            LeftMatchResult current = seekMatchResult(collection, starPos);
            while (current != null) {
                if (starPos == 0) {
                    last = new LeftMatchResult();
                    current.copyTo(last);
                } else {
                    last.append(current.getLeftMatchKeyList());
                    last.setCanAppendNewKey(current.getCanAppendNewKey());
                }

                starPos = starPos + current.getCount();
                if (current.getCanAppendNewKey()) {
                    current = seekMatchResult(collection, starPos);
                } else {
                    break;
                }
            }
            return last;
        }

        return collection.get(0);
    }
}
