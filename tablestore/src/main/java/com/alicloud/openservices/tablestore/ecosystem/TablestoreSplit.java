package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.iterator.RowIterator;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class TablestoreSplit implements ITablestoreSplit {
    private static final Logger LOGGER = LoggerFactory.getLogger(TablestoreSplit.class);

    private int maxFirstColumnRangeSupport = 20;
    private int maxFirstColumnEqualSupport = 10;
    private static ICatalogManager manager;
    private static Gson gson = new Gson();

    public TablestoreSplit(SplitType type, Filter filter, List<String> requiredColumns) {
        this.type = type;
        this.filter = filter;
        this.requiredColumns = requiredColumns;
    }

    public TablestoreSplit(SplitType type, Filter filter, List<String> requiredColumns, byte[] sessionId, int splitId) {
        this.type = type;
        this.filter = filter;
        this.requiredColumns = requiredColumns;
        this.sessionId = sessionId;
        this.splitId = splitId;
    }

    public TablestoreSplit(SplitType type, Filter filter, List<String> requiredColumns, byte[] sessionId, int splitId, int maxParallel, List<String> geoColumnNames) {
        this.type = type;
        this.filter = filter;
        this.requiredColumns = requiredColumns;
        this.sessionId = sessionId;
        this.splitId = splitId;
        this.maxParallel = maxParallel;
        this.geoColumnNames = geoColumnNames;
    }

    public enum SplitType {
        /**
         * split for kv
         */
        KeyValue,

        /**
         * split for search
         */
        SearchIndex
    }

    private TableMeta meta;
    private SearchInfo searchInfo;

    private String splitName;
    private String tableName;
    private SplitType type;
    private Split kvSplit;
    private Filter filter;

    // For SearchIndex parallel scan
    private byte[] sessionId;
    private int maxParallel;
    private int splitId;

    public static List<String> geoColumnNames;

    private List<String> requiredColumns;

    public static boolean containGeo = false;
    public static boolean containOr = false;
    private static boolean pushAll = false;

    public void setSplitName(String name) {
        this.splitName = name;
    }

    public List<String> getRequiredColumns() {
        return this.requiredColumns;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setKvSplit(Split split) {
        kvSplit = split;
    }

    public void setSearchInfo(SearchInfo info) {
        this.searchInfo = info;
    }

    public void setKvTableMeta(TableMeta meta) {
        this.meta = meta;
    }

    @Override
    public Iterator<Row> getRowIterator(SyncClientInterface client) {
        if (type == SplitType.KeyValue) {
            return generateTableIterator(client);
        } else {
            FilterPushdownConfig filterPushdownConfig = new FilterPushdownConfig();
            return generateSearchIndexIterator(client, filterPushdownConfig);
        }
    }

    @Override
    public Iterator<Row> getRowIterator(SyncClientInterface client, FilterPushdownConfig filterPushdownConfig) {
        if (type == SplitType.KeyValue) {
            return generateTableIterator(client);
        } else {
            return generateSearchIndexIterator(client, filterPushdownConfig);
        }
    }

    private Iterator<Row> generateTableIterator(SyncClientInterface client) {
        List<PkRange> pkRanges = generatePkRange(filter, kvSplit.getLowerBound(), kvSplit.getUpperBound());
        LOGGER.info("kvSplit lower bound: {}, upper bound: {}, pkRanges: {}",
                kvSplit.getLowerBound(), kvSplit.getUpperBound(), gson.toJson(pkRanges));
        if (checkIsBatchRead(pkRanges)) {
            LOGGER.debug("Batch get row");
            BatchGetRowRequest request = buildBatchGet(pkRanges);
            BatchGetRowResponse response = client.batchGetRow(request);
            // to do check result
            if (response.getFailedRows().isEmpty()) {
                List<Row> rowCollction = new ArrayList<Row>();
                for (BatchGetRowResponse.RowResult result : response.getSucceedRows()) {
                    if (result.getRow() != null) {
                        rowCollction.add(result.getRow());
                    }
                }
                return rowCollction.iterator();
            }
        } else {
            // todo : we can add several sub range if it's faster than the whole split
            if (pkRanges.size() == 1) {
                LOGGER.debug("Generate sub range to scan");
                PkRange range = pkRanges.get(0);
                return generateIterator(client, range.begin, range.end);
            } else if (pkRanges.size() > 1) {
                return new TablestoreSplitIterator(client, pkRanges, this.meta.getTableName(), requiredColumns);
            }
        }
        LOGGER.debug("Scan the whole split");
        return generateIterator(client, kvSplit.getLowerBound(), kvSplit.getUpperBound());
    }

    private boolean checkIsBatchRead(List<PkRange> pkRanges) {
        if (pkRanges.size() >= 1 && pkRanges.size() <= 100) {
            for (PkRange range : pkRanges) {
                if (range.equal == null) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private BatchGetRowRequest buildBatchGet(List<PkRange> pkRanges) {
        BatchGetRowRequest request = new BatchGetRowRequest();
        MultiRowQueryCriteria criteria1 = new MultiRowQueryCriteria(tableName);
        criteria1.setMaxVersions(1);
        for (PkRange range : pkRanges) {
            criteria1.addRow(range.equal);
            request.addMultiRowQueryCriteria(criteria1);
        }
        return request;
    }


    private List<String> removePrimaryKey(List<String> columnNames) {
        if (!tableName.isEmpty()) {
            this.meta = manager.getTableCatalog(tableName).getTableMeta();
            Map<String, PrimaryKeyType> primaryKeyTypeMap = meta.getPrimaryKeyMap();
            List<String> retColumns = new ArrayList<String>();
            for (String columnName : columnNames) {
                if (!primaryKeyTypeMap.containsKey(columnName)) {
                    retColumns.add(columnName);
                }
            }
            return retColumns;
        } else {
            return columnNames;
        }
    }

    private Iterator<Row> generateSearchIndexIterator(SyncClientInterface client, FilterPushdownConfig filterPushdownConfig) {
//        QueryBuilder query = buildSearchQueryFromFilter(filter);
        Filter flatFilter = buildFlatTreeFilter(filter);
        Query query = buildMergedSearchQueryFromFilter(flatFilter, filterPushdownConfig);
        ParallelScanRequest parallelScanRequestByBuilder = ParallelScanRequest.newBuilder()
                .tableName(tableName)
                .indexName(searchInfo.getSearchIndexName())
                .scanQuery(ScanQuery.newBuilder()
                        .query(query)
                        .limit(2000)
                        .currentParallelId(splitId)
                        .maxParallel(maxParallel)
                        .build())
                .addColumnsToGet(removePrimaryKey(requiredColumns))
                .sessionId(sessionId)
                .build();
        RowIterator iterator = client.createParallelScanIterator(parallelScanRequestByBuilder);
        return iterator;
    }

    private Iterator<Row> generateIterator(SyncClientInterface client, PrimaryKey begin, PrimaryKey end) {
        RangeIteratorParameter rangeIteratorParameter = new RangeIteratorParameter(this.meta.getTableName());
        rangeIteratorParameter.setInclusiveStartPrimaryKey(begin);
        rangeIteratorParameter.setExclusiveEndPrimaryKey(end);
        rangeIteratorParameter.setMaxVersions(1);
        if (requiredColumns != null && !requiredColumns.isEmpty()) {
            for (String col : requiredColumns) {
                rangeIteratorParameter.addColumnsToGet(col);
            }
        } else {
            String defaultString = begin.getPrimaryKeyColumn(0).getName();
            rangeIteratorParameter.addColumnsToGet(defaultString);
        }
        Iterator<Row> iterator = client.createRangeIterator(rangeIteratorParameter);
        return iterator;
    }

    public static class Range {
        public void setRightOpen(boolean rightOpen) {
            isRightOpen = rightOpen;
        }

        public boolean isRightOpen() {
            return isRightOpen;
        }

        boolean isRightOpen = false;

        public Range(Range range) {
            if (range.equal != null) {
                this.equal = range.equal;
            } else {
                this.begin = range.begin;
                this.end = range.end;
            }
            if (range.next != null) {
                this.next = range.next;
            }

            this.isRightOpen = range.isRightOpen;
        }

        public Range(PrimaryKeyColumn begin, PrimaryKeyColumn end) {
            this.begin = begin;
            this.end = end;
        }

        public Range(PrimaryKeyColumn begin, PrimaryKeyColumn end, boolean isRightOpen) {
            this.begin = begin;
            this.end = end;
            this.isRightOpen = isRightOpen;
        }

        public Range(PrimaryKeyColumn equal) {
            this.equal = equal;
        }

        public PrimaryKeyColumn getBegin() {
            return begin;
        }

        public PrimaryKeyColumn getEnd() {
            return end;
        }

        public PrimaryKeyColumn getEqual() {
            return equal;
        }

        public Range getNext() {
            return next;
        }

        public void setBegin(PrimaryKeyColumn begin) {
            this.begin = begin;
        }

        public void setEnd(PrimaryKeyColumn end) {
            this.end = end;
        }

        public void setEqual(PrimaryKeyColumn equal) {
            this.equal = equal;
        }

        public void setNext(Range next) {
            this.next = next;
        }

        private PrimaryKeyColumn begin;
        private PrimaryKeyColumn end;

        private PrimaryKeyColumn equal;
        private Range next;

        public boolean isSingleValue() {
            return this.equal != null;
        }
    }

    public static class PkRange {
        public PrimaryKey getBegin() {
            return begin;
        }

        public PrimaryKey getEnd() {
            return end;
        }

        public PrimaryKey getEqual() {
            return equal;
        }

        private PrimaryKey begin;
        private PrimaryKey end;

        private PrimaryKey equal;

        public PkRange(PrimaryKey begin, PrimaryKey end) {
            this.begin = begin;
            this.end = end;
        }

        public PkRange(PrimaryKey equal) {
            this.equal = equal;
        }

        public boolean isSingleValue() {
            return equal != null;
        }
    }

    private List<Range> generateColumnRange(Filter filter, PrimaryKey begin, PrimaryKey end, int starPos) {
        String name = this.meta.getPrimaryKeyList().get(starPos).getName();

        if (filter.isNested()) {
            if (filter.getLogicOperator() == Filter.LogicOperator.AND || filter.getLogicOperator() == Filter.LogicOperator.OR) {
                List<Range> mergedResult = new ArrayList<Range>();
                for (Filter subFilter : filter.getSubFilters()) {
                    List<Range> result = generateColumnRange(subFilter, begin, end, starPos);
                    if (mergedResult.isEmpty() && result != null && !result.isEmpty()) {
                        mergedResult = result;
                        continue;
                    }

                    if (filter.getLogicOperator() == Filter.LogicOperator.AND) {
                        if (result != null && !result.isEmpty()) {
                            mergedResult = mergeSubRange(mergedResult, result, begin, end);
                        }
                        if (result == null) {
                            return new ArrayList<Range>();
                        }
                    } else {
                        if (result != null && !result.isEmpty()) {
                            mergedResult = mergeTotalRange(mergedResult, result, begin, end);
                        }
                    }
                }
                return mergedResult;
            } else {
                PrimaryKeyColumn col1 = new PrimaryKeyColumn(name, begin.getPrimaryKeyColumn(starPos).getValue());
                PrimaryKeyColumn col2 = new PrimaryKeyColumn(name, end.getPrimaryKeyColumn(starPos).getValue());

                Range range = new Range(col1, col2);
                return Arrays.asList(range);
            }
        } else {
            List<Range> result = new ArrayList<Range>();
            if (filter != null && filter.getColumnName() != null && filter.getColumnName().equals(name)) {
                PrimaryKeyValue pkv = PrimaryKeyValue.fromColumn(filter.getColumnValue());
                if (filter.getCompareOperator() == Filter.CompareOperator.EQUAL) {
                    PrimaryKeyColumn pkc = new PrimaryKeyColumn(name, PrimaryKeyValue.fromColumn(filter.getColumnValue()));
                    Range range = new Range(pkc);
                    result.add(range);
                } else if (filter.getCompareOperator() == Filter.CompareOperator.LESS_THAN ||
                        filter.getCompareOperator() == Filter.CompareOperator.LESS_EQUAL) {
                    boolean isRightOpen = filter.getCompareOperator() == Filter.CompareOperator.LESS_THAN;
                    if (starPos == 0) {
                        if (pkv.compareTo(end.getPrimaryKeyColumn(starPos).getValue()) >= 0) {
                            PrimaryKeyColumn pkc1 = new PrimaryKeyColumn(name, begin.getPrimaryKeyColumn(starPos).getValue());
                            PrimaryKeyColumn pkc2 = new PrimaryKeyColumn(name, end.getPrimaryKeyColumn(starPos).getValue());
                            Range range = new Range(pkc1, pkc2, isRightOpen);
                            result.add(range);
                        } else if (pkv.compareTo(begin.getPrimaryKeyColumn(starPos).getValue()) >= 0) {
                            PrimaryKeyColumn pkc1 = new PrimaryKeyColumn(name, begin.getPrimaryKeyColumn(starPos).getValue());
                            PrimaryKeyColumn pkc2 = new PrimaryKeyColumn(name, pkv);
                            Range range = new Range(pkc1, pkc2, isRightOpen);
                            result.add(range);
                        } else {
                            //
                            LOGGER.info("find empty split", begin.toString(), end.toString());
                        }
                    } else {
                        PrimaryKeyColumn pkc1 = new PrimaryKeyColumn(name, PrimaryKeyValue.INF_MIN);
                        PrimaryKeyColumn pkc2 = new PrimaryKeyColumn(name, pkv);
                        Range range = new Range(pkc1, pkc2, isRightOpen);
                        result.add(range);
                    }
                } else if (filter.getCompareOperator() == Filter.CompareOperator.GREATER_EQUAL ||
                        filter.getCompareOperator() == Filter.CompareOperator.GREATER_THAN) {
                    if (starPos == 0) {
                        if (pkv.compareTo(begin.getPrimaryKeyColumn(starPos).getValue()) < 0) {
                            PrimaryKeyColumn pkc1 = new PrimaryKeyColumn(name, begin.getPrimaryKeyColumn(starPos).getValue());
                            PrimaryKeyColumn pkc2 = new PrimaryKeyColumn(name, end.getPrimaryKeyColumn(starPos).getValue());
                            Range range = new Range(pkc1, pkc2);
                            result.add(range);
                        } else if (pkv.compareTo(end.getPrimaryKeyColumn(starPos).getValue()) <= 0) {
                            PrimaryKeyColumn pkc1 = new PrimaryKeyColumn(name, pkv);
                            PrimaryKeyColumn pkc2 = new PrimaryKeyColumn(name, end.getPrimaryKeyColumn(starPos).getValue());
                            Range range = new Range(pkc1, pkc2);
                            result.add(range);
                        }
                    } else {
                        PrimaryKeyColumn pkc1 = new PrimaryKeyColumn(name, pkv);
                        PrimaryKeyColumn pkc2 = new PrimaryKeyColumn(name, PrimaryKeyValue.INF_MAX);
                        Range range = new Range(pkc1, pkc2);
                        result.add(range);
                    }
                }
            }
            return result;
        }
    }

    public List<PkRange> generatePkRange(Filter filter, PrimaryKey begin, PrimaryKey end) {
        List<PkRange> pkList = new ArrayList<PkRange>();
        List<Range> first = generateColumnRange(filter, begin, end, 0);
        LOGGER.info("First generate column ranges: {}", gson.toJson(first));
        boolean isRange = false;
        if (first.size() > maxFirstColumnEqualSupport && first.size() <= maxFirstColumnRangeSupport) {
            isRange = true;
        } else if (first.size() > maxFirstColumnRangeSupport) {
            PkRange pkRange = new PkRange(begin, end);
            pkList.add(pkRange);
            return pkList;
        } else {
            isRange = checkHasRange(first);
        }
        if (isRange) {
            return buildPkRangeFromRange(first, begin, end);
        } else {
            if (this.meta.getPrimaryKeyList().size() == 1) {
                return buildPkRangeFromRange(first, begin, end);
            }
            for (int i = 1; i < this.meta.getPrimaryKeyList().size(); i++) {
                List<Range> subRange = generateColumnRange(filter, begin, end, i);
                if (checkHasRange(subRange) || i == this.meta.getPrimaryKeyList().size() - 1 || subRange.isEmpty()) {
                    first = appendRange(first, subRange);
                    return buildPkRangeFromRange(first, begin, end);
                } else {
                    first = appendRange(first, subRange);
                }
            }
        }
        return pkList;
    }

    private List<Range> appendRange(List<Range> rangeList1, List<Range> rangeList2) {
        List<Range> rangeList = new ArrayList<Range>();
        for (Range front : rangeList1) {
            if (rangeList2.isEmpty()) {
                Range temp = new Range(front);
                rangeList.add(temp);
            } else {
                for (Range back : rangeList2) {
                    Range temp = new Range(front);
                    Range temp2 = temp;
                    while (temp2.next != null) {
                        temp2 = temp2.next;
                    }
                    temp2.next = back;
                    rangeList.add(temp);
                }
            }
        }

        return rangeList;
    }

    private List<PkRange> buildPkRangeFromRange(List<Range> rangeList, PrimaryKey begin, PrimaryKey end) {
        List<PkRange> pkList = new ArrayList<PkRange>();
        int pkCount = this.meta.getPrimaryKeyList().size();
        for (Range range : rangeList) {
            LOGGER.info("build rangeBegin: {}, rangeEnd: {}, rangeEqual: {}, PrimaryKeyEnd:{}",
                    range.begin, range.end, range.equal, end);
            PrimaryKeyBuilder pkb = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            PrimaryKeyBuilder pkb2 = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            PrimaryKey equalPk = null;
            boolean isAllEqual = false;
            boolean isRange = false;
            if (range.equal != null) {
                pkb.addPrimaryKeyColumn(range.equal.getName(), range.equal.getValue());
                if (pkCount > 1) {
                    pkb2.addPrimaryKeyColumn(range.equal.getName(), range.equal.getValue());
                } else {
                    equalPk = pkb.build();
                    isAllEqual = true;
                }
            } else {
                isRange = true;
                pkb.addPrimaryKeyColumn(range.begin.getName(), range.begin.getValue());
                pkb2.addPrimaryKeyColumn(range.end.getName(), range.end.getValue());
                if (pkCount == 1 && !range.isRightOpen()) {
                    equalPk = pkb2.build();
                }
            }

            int index = 0;
            Range currentColumn = range.next;
            int equalCursor = 0;
            for (PrimaryKeySchema pkSchema : this.meta.getPrimaryKeyList()) {
                if (index > 0) {
                    if (isRange || currentColumn == null) {
                        pkb.addPrimaryKeyColumn(pkSchema.getName(), PrimaryKeyValue.INF_MIN);
                        if (range.getEqual() != null && !range.getEqual().getValue().equals(end.getPrimaryKeyColumn(equalCursor).getValue())) {
                            pkb2.addPrimaryKeyColumn(pkSchema.getName(), PrimaryKeyValue.INF_MAX);
                        } else if (range.getEqual() != null && range.getEqual().getValue().equals(end.getPrimaryKeyColumn(equalCursor).getValue())) {
                            pkb2.addPrimaryKeyColumn(pkSchema.getName(), PrimaryKeyValue.INF_MIN);
                        } else if (range.getEnd().getValue().equals(end.getPrimaryKeyColumn(equalCursor).getValue())) {
                            pkb2.addPrimaryKeyColumn(pkSchema.getName(), PrimaryKeyValue.INF_MIN);
                        } else {
                            if (range.isRightOpen) {
                                pkb2.addPrimaryKeyColumn(pkSchema.getName(), PrimaryKeyValue.INF_MIN);
                            } else {
                                pkb2.addPrimaryKeyColumn(pkSchema.getName(), PrimaryKeyValue.INF_MAX);
                            }
                        }

                        isRange = true;
                    } else {
                        if (currentColumn.equal != null) {
                            pkb.addPrimaryKeyColumn(pkSchema.getName(), currentColumn.equal.getValue());
                            if (index < pkCount - 1) {
                                pkb2.addPrimaryKeyColumn(pkSchema.getName(), currentColumn.equal.getValue());
                            } else {
                                equalPk = pkb.build();
                                isAllEqual = true;
                            }
                        } else {
                            pkb.addPrimaryKeyColumn(pkSchema.getName(), currentColumn.begin.getValue());
                            pkb2.addPrimaryKeyColumn(pkSchema.getName(), currentColumn.end.getValue());
                            isRange = true;

                            if (index == pkCount - 1) {
                                PrimaryKey temp = pkb2.build();
                                if (temp.compareTo(end) < 0 && currentColumn.end.getValue() != PrimaryKeyValue.INF_MAX && currentColumn.end.getValue() != PrimaryKeyValue.INF_MIN) {
                                    if (!currentColumn.isRightOpen()) {
                                        equalPk = pkb2.build();
                                    }
                                }
                            }
                        }
                        range = currentColumn;
                        currentColumn = currentColumn.next;
                        equalCursor++;
                    }
                }
                index++;
            }
            PkRange pkRange;
            PrimaryKey beginPk = pkb.build();
            if (isRange) {
                PrimaryKey endPk = pkb2.build();
                pkRange = new PkRange(beginPk, endPk);
            } else {
                pkRange = new PkRange(beginPk);
            }
            if (!isAllEqual) {
                pkList.add(pkRange);
            }

            if (equalPk != null) {
                PkRange pkRange2 = new PkRange(equalPk);
                pkList.add(pkRange2);
            }

        }
        // filter invalid PKRange.
        return filterPkRanges(pkList, begin, end);
    }

    public List<PkRange> filterPkRanges(List<PkRange> pkList, PrimaryKey begin, PrimaryKey end) {
        List<PkRange> retRanges = new ArrayList<PkRange>();
        for (PkRange pkRange : pkList) {
            if (pkRange.isSingleValue()) {
                if (pkRange.getEqual().compareTo(begin) >= 0 && pkRange.getEqual().compareTo(end) < 0) {
                    retRanges.add(pkRange);
                } else {
                    LOGGER.info("Filter invalid pkrange: {}, begin: {}, end: {}",
                            gson.toJson(pkRange), begin.jsonize(), end.jsonize());
                }
            } else {
                if (pkRange.getBegin().compareTo(pkRange.getEnd()) < 0 &&
                        pkRange.getBegin().compareTo(begin) >= 0 && pkRange.getEnd().compareTo(end) <= 0) {
                    retRanges.add(pkRange);
                } else {
                    LOGGER.info("Filter invalid pkrange: {}, begin: {}, end: {}",
                            gson.toJson(pkRange), begin.jsonize(), end.jsonize());
                }
            }
        }
        return retRanges;
    }

    private boolean checkHasRange(List<Range> rangeList) {
        for (Range range : rangeList) {
            if (!range.isSingleValue()) {
                // means at least one sub split is range
                return true;
            }
        }
        return false;
    }

    private List<Range> mergeSubRange(List<Range> range1, List<Range> range2, PrimaryKey begin, PrimaryKey end) {
        for (Range subRange : range2) {
            range1 = mergeSubRange(range1, subRange, begin, end);
        }
        return range1;
    }

    private List<Range> mergeSubRange(List<Range> range1, Range subRange, PrimaryKey begin, PrimaryKey end) {
        List<Range> retRange = new ArrayList<Range>();
        for (Range range : range1) {
            LOGGER.info("rangeBegin: {}, rangeEnd: {}, rangeEqual: {}, subRangeBegin: {}, subRangeEnd: {}, subRangeEqual: {}",
                    range.begin, range.end, range.equal, subRange.begin, subRange.end, subRange.equal);
            if (range.equal != null) {
                return range1;
            } else if (subRange.equal != null) {
                List<Range> rangeList = new ArrayList<Range>();
                rangeList.add(subRange);
                return rangeList;
            } else {
                PrimaryKeyColumn pkcBegin = range.begin;
                PrimaryKeyColumn pkcEnd = range.end;

                if (subRange.begin.getName().equals(pkcBegin.getName())) {
                    if (pkcEnd.getValue().compareTo(subRange.begin.getValue()) < 0 ||
                            pkcBegin.getValue().compareTo(subRange.end.getValue()) > 0) {
                        continue;
                    }
                    if (pkcBegin.getValue().compareTo(subRange.begin.getValue()) > 0
                            && pkcBegin.getValue().compareTo(subRange.end.getValue()) < 0
                            && pkcEnd.getValue().compareTo(subRange.end.getValue()) > 0) {
                        range.setEnd(subRange.end);
                        range.setRightOpen(subRange.isRightOpen);
                    } else if (pkcBegin.getValue().compareTo(subRange.begin.getValue()) < 0
                            && pkcEnd.getValue().compareTo(subRange.begin.getValue()) > 0) {
                        if (pkcEnd.getValue().compareTo(subRange.end.getValue()) > 0) {
                            range.setEnd(subRange.end);
                            range.setRightOpen(subRange.isRightOpen);
                            range.setBegin(subRange.begin);
                        }
                        if (pkcEnd.getValue().compareTo(subRange.end.getValue()) <= 0) {
                            range.setBegin(subRange.begin);
                        }
                    }

                    retRange.add(range);
                   /* else if (pkcBegin.getValue().compareTo(subRange.end.getValue()) == 0) {

                    }*/
                } else {
                    return range1;
                }
            }
        }
        return retRange;

    }

    private List<Range> mergeTotalRange(List<Range> range1, List<Range> range2, PrimaryKey begin, PrimaryKey end) {
        range1.addAll(range2);
        return range1;
    }

    @Override
    public void initial(SyncClient client) {
        if (manager == null) {
            synchronized (TablestoreSplit.class) {
                if (manager == null) {
                    manager = new CatalogManager(client);
                }
            }
        }
        TableCatalog catalog = manager.getTableCatalog(tableName);
        if (type == SplitType.KeyValue) {
            if (tableName.equals(splitName)) {
                this.meta = catalog.getTableMeta();
            } else {
                List<TableMeta> metas = catalog.getIndexMetaList();
                for (TableMeta tempMeta : metas) {
                    if (tempMeta.getTableName().equals(splitName)) {
                        this.meta = tempMeta;
                        return;
                    }
                }
                throw new IllegalArgumentException("invalid split name");
            }
        } else {
            List<IndexSchema> metas = catalog.getSearchSchema();
            this.searchInfo = new SearchInfo(splitName, metas.get(0));
            return;
        }
    }

    /**
     * @param filter true means this split may have some data left after the filter
     * @return
     */
    public boolean checkIfMatchTheFilter(Filter filter) {
        if (filter.isNested()) {
            if (filter.getLogicOperator() == Filter.LogicOperator.AND) {
                for (Filter subFilter : filter.getSubFilters()) {
                    if (!checkIfMatchTheFilter(subFilter)) {
                        return false;
                    }
                }
                return true;
            } else if (filter.getLogicOperator() == Filter.LogicOperator.OR) {
                for (Filter subFilter : filter.getSubFilters()) {
                    if (checkIfMatchTheFilter(subFilter)) {
                        return true;
                    }
                }
                return false;
            } else {
                //
                return true;
            }
        } else {
            PrimaryKey pk = kvSplit.getLowerBound();
            if (filter.getCompareOperator() == Filter.CompareOperator.NOT_EQUAL) {
                return true;
            } else if (Filter.CompareOperator.EQUAL == filter.getCompareOperator()) {
                if (!filter.getColumnName().equals(pk.getPrimaryKeyColumn(0).getName())) {
                    return true;
                }
                try {
                    return (kvSplit.getLowerBound().getPrimaryKeyColumn(0).getValue().isInfMin() || kvSplit.getLowerBound().getPrimaryKeyColumn(0).getValue().toColumnValue().compareTo(filter.getColumnValue()) <= 0)
                            && (kvSplit.getUpperBound().getPrimaryKeyColumn(0).getValue().isInfMax() || kvSplit.getUpperBound().getPrimaryKeyColumn(0).getValue().toColumnValue().compareTo(filter.getColumnValue()) > 0);
                } catch (IOException e) {
                    e.printStackTrace();
                    return true;
                }
            } else if (Filter.CompareOperator.EMPTY_FILTER == filter.getCompareOperator()) {
                return true;
            }

            if (filter.getColumnName().equals(pk.getPrimaryKeyColumn(0).getName())) {
                if (filter.getCompareOperator() == Filter.CompareOperator.GREATER_THAN ||
                        filter.getCompareOperator() == Filter.CompareOperator.GREATER_EQUAL) {
                    try {

                        return (kvSplit.getUpperBound().getPrimaryKeyColumn(0).getValue().isInfMax()
                                || kvSplit.getUpperBound().getPrimaryKeyColumn(0).getValue().toColumnValue().compareTo(filter.getColumnValue()) > 0);
                    } catch (IOException e) {
                        return true;
                    }

                } else if (filter.getCompareOperator() == Filter.CompareOperator.LESS_THAN ||
                        filter.getCompareOperator() == Filter.CompareOperator.LESS_EQUAL) {
                    try {
                        if (filter.getCompareOperator() == Filter.CompareOperator.LESS_THAN) {
                            return (kvSplit.getLowerBound().getPrimaryKeyColumn(0).getValue().isInfMin()
                                    || kvSplit.getLowerBound().getPrimaryKeyColumn(0).getValue().toColumnValue().compareTo(filter.getColumnValue()) < 0);
                        } else {
                            return (kvSplit.getLowerBound().getPrimaryKeyColumn(0).getValue().isInfMin()
                                    || kvSplit.getLowerBound().getPrimaryKeyColumn(0).getValue().toColumnValue().compareTo(filter.getColumnValue()) <= 0);
                        }

                    } catch (IOException e) {
                        return true;
                    }
                }
            }
            return true;
        }
    }

    public SplitType getType() {
        return type;
    }

    public Filter getFilter() {
        return filter;
    }


    public String getSplitName() {
        return splitName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setType(SplitType type) {
        this.type = type;
    }

    public Split getKvSplit() {
        return kvSplit;
    }

    public void setFilter(Filter filter) {
    }

    public byte[] getSessionId() {
        return sessionId;
    }

    public int getSplitId() {
        return splitId;
    }

    public List<String> getGeoColumnNames() {
        return geoColumnNames;
    }

    public int getMaxParallel() {
        return maxParallel;
    }

    @Override
    public String toString() {
        return "Name:" + tableName + ", splitname" + splitName + ",type:" + type;
    }

    // Flat the tree filter
    public Filter buildFlatTreeFilter(Filter filter) {
        if (filter.isNested()) {
            List<Filter> origFilters = new ArrayList<Filter>();
            for (Filter subFilter : filter.getSubFilters()) {
                origFilters.add(buildFlatTreeFilter(subFilter));
            }

            List<Filter> flatFilters = new ArrayList<Filter>();
            for (Filter subFilter : origFilters) {
                if (subFilter.isNested() && filter.getLogicOperator() != Filter.LogicOperator.NOT
                        && filter.getLogicOperator() == subFilter.getLogicOperator()) {
                    flatFilters.addAll(subFilter.getSubFilters());
                } else {
                    flatFilters.add(subFilter);
                }
            }
            return new Filter(filter.getLogicOperator(), flatFilters);
        } else {
            return filter;
        }
    }

    // Merged the same tree level query conditions.
    public List<Query> mergeQuerys(List<Query> origQuerys) {
        List<Query> remainQuerys = new ArrayList<Query>();
        Map<String, RangeQuery> mergedRangeQuerys = new HashMap<String, RangeQuery>(origQuerys.size());
        for (Query query : origQuerys) {
            if (query.getQueryType() == QueryType.QueryType_RangeQuery) {
                RangeQuery rangeQuery = (RangeQuery) query;
                RangeQuery mergedRangeQuery = mergedRangeQuerys.get(rangeQuery.getFieldName());
                if (mergedRangeQuery != null) {
                    if (rangeQuery.getFrom() != null) {
                        if (mergedRangeQuery.getFrom() == null) {
                            mergedRangeQuery.setFrom(rangeQuery.getFrom());
                            mergedRangeQuery.setIncludeLower(rangeQuery.isIncludeLower());
                        } else if (rangeQuery.getFrom().compareTo(mergedRangeQuery.getFrom()) > 0) {
                            mergedRangeQuery.setFrom(rangeQuery.getFrom());
                            mergedRangeQuery.setIncludeLower(rangeQuery.isIncludeLower());
                        } else if (rangeQuery.getFrom().compareTo(mergedRangeQuery.getFrom()) == 0) {
                            // a >= 5 AND a > 5 => a > 5
                            if (!rangeQuery.isIncludeLower() || !mergedRangeQuery.isIncludeLower()) {
                                mergedRangeQuery.setIncludeLower(false);
                            }
                        }
                    }

                    if (rangeQuery.getTo() != null) {
                        if (mergedRangeQuery.getTo() == null) {
                            mergedRangeQuery.setTo(rangeQuery.getTo());
                            mergedRangeQuery.setIncludeUpper(rangeQuery.isIncludeUpper());
                        } else if (rangeQuery.getTo().compareTo(mergedRangeQuery.getTo()) < 0) {
                            mergedRangeQuery.setTo(rangeQuery.getTo());
                            mergedRangeQuery.setIncludeUpper(rangeQuery.isIncludeUpper());
                        } else if (rangeQuery.getTo().compareTo(mergedRangeQuery.getTo()) == 0) {
                            // a <= 5 AND a < 5 => a <= 5
                            if (!rangeQuery.isIncludeUpper() || !mergedRangeQuery.isIncludeUpper()) {
                                mergedRangeQuery.setIncludeUpper(false);
                            }
                        }
                        // unnecessary
                        if (mergedRangeQuery.getTo() == null ||
                                (mergedRangeQuery.getTo() != null && rangeQuery.getTo().compareTo(mergedRangeQuery.getTo()) <= 0)) {
                            mergedRangeQuery.setTo(rangeQuery.getTo());
                        }
                    }

                } else {
                    mergedRangeQuerys.put(rangeQuery.getFieldName(), rangeQuery);
                }
            } else {
                remainQuerys.add(query);
            }
        }

        List<Query> retQuerys = new ArrayList<Query>();
        retQuerys.addAll(mergedRangeQuerys.values());
        retQuerys.addAll(remainQuerys);
        return retQuerys;
    }

    public static Filter getUnhandledOtsFilter(SyncClient client, Filter filter, String tableName, String indexName) {
        FilterPushdownConfig filterPushdownConfig = new FilterPushdownConfig();
        return getUnhandledOtsFilterCore(client, filter, tableName, indexName, filterPushdownConfig);
    }


    public static Filter getUnhandledOtsFilter(SyncClient client, Filter filter, String tableName, String indexName, FilterPushdownConfig filterPushdownConfig) {
        return getUnhandledOtsFilterCore(client, filter, tableName, indexName, filterPushdownConfig);
    }


    public static Filter getUnhandledOtsFilterCore(SyncClient client, Filter filter, String tableName, String indexName, FilterPushdownConfig filterPushdownConfig) {
        boolean pushRangeLong = filterPushdownConfig.pushRangeLong;
        boolean pushRangeString = filterPushdownConfig.pushRangeString;

        if (geoColumnNames == null) {
            geoColumnNames = generateGeoColumnNames(client, tableName, indexName);
        }

        if (filter.isNested()) {
            List<Filter> subFiltersOld = filter.getFilters();
            List<Filter> subFilterNewList = new ArrayList<Filter>();
            if (filter.getLogicOperator() == Filter.LogicOperator.OR) {
                containOr = true;
                if (!pushRangeLong && containsRangeLong || !pushRangeString && containsRangeString) {
                    LOGGER.warn("push.down.range.long push.down.range.string will be regarded as true when sql contains OR, so all filters will be pushed down");
                }
            }
            for (Filter subFilter : subFiltersOld) {
                Filter subFilterNew = getUnhandledOtsFilterCore(client, subFilter, tableName, indexName, filterPushdownConfig);
                if (subFilterNew != null) {
                    subFilterNewList.add(subFilterNew);
                }
            }
            if (subFilterNewList.size() >= 2) {
                filter.setFilters(subFilterNewList);
            } else if (subFilterNewList.size() == 1) {
                filter = subFilterNewList.get(0);
            } else {
                filter = null;
            }
            return filter;
        } else {
            ColumnValue filterColumnValue = filter.getColumnValue();
            if (geoColumnNames.contains(filter.getColumnName())) {
                containGeo = true;
                return null;
            } else if (filter.getCompareOperator() == Filter.CompareOperator.GREATER_THAN ||
                    filter.getCompareOperator() == Filter.CompareOperator.GREATER_EQUAL ||
                    filter.getCompareOperator() == Filter.CompareOperator.LESS_THAN ||
                    filter.getCompareOperator() == Filter.CompareOperator.LESS_EQUAL) {
                if (filterColumnValue.getType() == ColumnType.INTEGER) {
                    containsRangeLong = true;
                }
                if (filterColumnValue.getType() == ColumnType.STRING) {
                    containsRangeString = true;
                }
                if (containOr) {
                    if (!pushRangeLong && containsRangeLong || !pushRangeString && containsRangeString) {
                        LOGGER.warn("push.down.range.long push.down.range.string will be regarded as true when sql contains OR, so all filters will be pushed down");
                    }
                    return null;
                } else {
                    if ((!pushRangeLong && filterColumnValue.getType() == ColumnType.INTEGER) ||
                            (!pushRangeString && filterColumnValue.getType() == ColumnType.STRING)) {
                        return filter;
                    } else {
                        return null;
                    }
                }
            } else {
                //other has been pushed down filter : start  with isnull  equal and so on
                return null;
            }
        }

    }

    private static List<String> generateGeoColumnNames(SyncClient client, String tableName, String indexName) {
        ArrayList<String> geoColumnNames = new ArrayList<String>();
        DescribeSearchIndexRequest request = new DescribeSearchIndexRequest();
        request.setTableName(tableName);
        request.setIndexName(indexName);

        DescribeSearchIndexResponse response = client.describeSearchIndex(request);
        IndexSchema indexSchema = response.getSchema();

        for (FieldSchema fieldSchema : indexSchema.getFieldSchemas()) {
            if (fieldSchema.getFieldType() == FieldType.GEO_POINT) {
                geoColumnNames.add(fieldSchema.getFieldName());
            }
        }

        return geoColumnNames;
    }

    public Query buildMergedSearchQueryFromFilter(Filter filter) throws Exception {
        return buildMergedSearchQueryFromFilter(filter, new FilterPushdownConfig());
    }

    private static boolean containsRangeLong = false;
    private static boolean containsRangeString = false;

    public Query buildMergedSearchQueryFromFilter(Filter filter, FilterPushdownConfig filterPushdownConfig) {
        boolean pushRangeLong = filterPushdownConfig.pushRangeLong;
        boolean pushRangeString = filterPushdownConfig.pushRangeString;
        if (filter.isNested()) {
            BoolQuery boolQuery = new BoolQuery();
            List<Query> mustQuerys = new ArrayList<Query>();
            List<Query> shouldQuerys = new ArrayList<Query>();
            List<Query> mustNotQuerys = new ArrayList<Query>();
            for (Filter subFilter : filter.getSubFilters()) {
                Query subQuery = buildMergedSearchQueryFromFilter(subFilter, filterPushdownConfig);
                if (filter.getLogicOperator() == Filter.LogicOperator.AND) {
                    mustQuerys.add(subQuery);
                } else if (filter.getLogicOperator() == Filter.LogicOperator.OR) {
                    shouldQuerys.add(subQuery);
                    containOr = true;
                    if (!pushRangeLong && containsRangeLong || !pushRangeString && containsRangeString) {
                        LOGGER.warn("push.down.range.long push.down.range.string will be regarded as true when sql contains OR");
                    }
                } else if (filter.getLogicOperator() == Filter.LogicOperator.NOT) {
                    mustNotQuerys.add(subQuery);
                }
            }

            // Merged the mustQuerys
            List<Query> mergedMustQuerys = mergeQuerys(mustQuerys);
            boolQuery.setMustQueries(mergedMustQuerys);
            boolQuery.setShouldQueries(shouldQuerys);
            boolQuery.setMustNotQueries(mustNotQuerys);
            return boolQuery;
        } else {
            ColumnValue filterColumnValue = filter.getColumnValue();
            if (geoColumnNames != null && geoColumnNames.contains(filter.getColumnName())) {
                Query geoQuery = getGeoQuery(filter);
                if (geoQuery != null) {
                    return geoQuery;
                }
            } else if (filter.getCompareOperator() == Filter.CompareOperator.EQUAL) {
                return QueryBuilders.term(filter.getColumnName(), filterColumnValue.getValue()).build();
            } else if (filter.getCompareOperator() == Filter.CompareOperator.IN) {
                return getInQuery(filter);
            } else if (filter.getCompareOperator() == Filter.CompareOperator.IS_NULL) {
                return getIsNullQuery(filter);
            } else if (filter.getCompareOperator() == Filter.CompareOperator.NOT_EQUAL) {
                return getNotEqualQuery(filter, filterColumnValue);
            } else if (filter.getCompareOperator() == Filter.CompareOperator.START_WITH) {
                return getStartWithQuery(filter, filterColumnValue);
            } else if (filter.getCompareOperator() == Filter.CompareOperator.GREATER_THAN ||
                    filter.getCompareOperator() == Filter.CompareOperator.GREATER_EQUAL ||
                    filter.getCompareOperator() == Filter.CompareOperator.LESS_THAN ||
                    filter.getCompareOperator() == Filter.CompareOperator.LESS_EQUAL) {
                Query rangeQuery = getRangeQuery(filter, pushRangeLong, pushRangeString, filterColumnValue);
                if (rangeQuery != null) {
                    return rangeQuery;
                }
            }
        }
        return new MatchAllQuery();
    }

    private Query getRangeQuery(Filter filter, boolean pushRangeLong, boolean pushRangeString, ColumnValue filterColumnValue) {
        if (filterColumnValue.getType() == ColumnType.INTEGER) {
            containsRangeLong = true;
        }
        if (filterColumnValue.getType() == ColumnType.STRING) {
            containsRangeString = true;
        }

        if (containOr) {
            if (!pushRangeLong && containsRangeLong || !pushRangeString && containsRangeString) {
                LOGGER.warn("push.down.range.long push.down.range.string will be regarded as true when sql contains or");
            }
            RangeQuery.Builder builder = QueryBuilders.range(filter.getColumnName());
            if (filter.getCompareOperator() == Filter.CompareOperator.GREATER_THAN) {
                return builder.greaterThan(filterColumnValue.getValue()).build();
            } else if (filter.getCompareOperator() == Filter.CompareOperator.GREATER_EQUAL) {
                return builder.greaterThanOrEqual(filterColumnValue.getValue()).build();
            } else if (filter.getCompareOperator() == Filter.CompareOperator.LESS_THAN) {
                return builder.lessThan(filterColumnValue.getValue()).build();
            } else if (filter.getCompareOperator() == Filter.CompareOperator.LESS_EQUAL) {
                return builder.lessThanOrEqual(filterColumnValue.getValue()).build();
            }
        } else {
            if (!pushRangeLong && filterColumnValue.getType() == ColumnType.INTEGER) {
                return new MatchAllQuery();
            }
            if (!pushRangeString && filterColumnValue.getType() == ColumnType.STRING) {
                return new MatchAllQuery();
            }
            RangeQuery.Builder builder = QueryBuilders.range(filter.getColumnName());
            if (filter.getCompareOperator() == Filter.CompareOperator.GREATER_THAN) {
                return builder.greaterThan(filterColumnValue.getValue()).build();
            } else if (filter.getCompareOperator() == Filter.CompareOperator.GREATER_EQUAL) {
                return builder.greaterThanOrEqual(filterColumnValue.getValue()).build();
            } else if (filter.getCompareOperator() == Filter.CompareOperator.LESS_THAN) {
                return builder.lessThan(filterColumnValue.getValue()).build();
            } else if (filter.getCompareOperator() == Filter.CompareOperator.LESS_EQUAL) {
                return builder.lessThanOrEqual(filterColumnValue.getValue()).build();
            }
        }
        return null;
    }

    private Query getStartWithQuery(Filter filter, ColumnValue filterColumnValue) {
        return QueryBuilders.prefix(filter.getColumnName(), filterColumnValue.asString()).build();
    }

    private Query getNotEqualQuery(Filter filter, ColumnValue filterColumnValue) {
        BoolQuery.Builder boolBuilder = QueryBuilders.bool();
        TermQuery.Builder termBuilder = QueryBuilders.term(filter.getColumnName(), filterColumnValue.getValue());
        return boolBuilder.mustNot(termBuilder).build();
    }

    private Query getIsNullQuery(Filter filter) {
        BoolQuery.Builder boolBuilder = QueryBuilders.bool();
        ExistsQuery.Builder existsBuilder = QueryBuilders.exists(filter.getColumnName());
        return boolBuilder.mustNot(existsBuilder).build();
    }

    private Query getInQuery(Filter filter) {
        TermsQuery.Builder terms = QueryBuilders.terms(filter.getColumnName()).terms();
        for (ColumnValue columnValue : filter.getColumnValuesForInOperator()) {
            terms.addTerm(columnValue.getValue());
        }
        return terms.build();
    }

    private Query getGeoQuery(Filter filter) {
        GeoQueryHelper query;
        try {
            query = GeoQueryHelper.buildGeoQueryHelper(filter);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            return null;
        }
        switch (query.getGeoType()) {
            case DISTANCE:
                return QueryBuilders.geoDistance(filter.getColumnName()).centerPoint(query.getCenterPoint())
                        .distanceInMeter(query.getDistanceInMeter()).build();
            case BOUNDING_BOX:
                return QueryBuilders.geoBoundingBox(filter.getColumnName()).topLeft(query.getTopLeft())
                        .bottomRight(query.getBottomRight()).build();
            case POLYGON:
                GeoPolygonQuery.Builder builder = QueryBuilders.geoPolygon(filter.getColumnName());
                for (String point : query.getPoints()) {
                    builder.addPoint(point);
                }
                return builder.build();
            default:
                return null;
        }
    }
}

