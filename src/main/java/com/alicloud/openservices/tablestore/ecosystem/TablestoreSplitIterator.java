package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.iterator.RowIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TablestoreSplitIterator implements RowIterator {

    private ArrayList<Iterator<Row>> rowIterators;

    private ArrayList<PrimaryKey> getRows;
    private Iterator<Row> current;
    private int iteratorIndex;
    private boolean needFetchSingleRow;
    public TablestoreSplitIterator(SyncClientInterface client, List<TablestoreSplit.PkRange> pkRanges, String tableName, List<String> requiredColumns) {
        rowIterators = new ArrayList<Iterator<Row>>();
        for(TablestoreSplit.PkRange pkRange : pkRanges) {
            if(pkRange.getEqual() == null) {
                Iterator<Row> rows= generateIterator(client, pkRange.getBegin(),pkRange.getEnd(), tableName, requiredColumns);
                rowIterators.add(rows);
            } else {
                needFetchSingleRow = true;
            }
        }

        if (needFetchSingleRow) {
            BatchGetRowRequest request = buildBatchGet(pkRanges, tableName);
            BatchGetRowResponse response = client.batchGetRow(request);
            if (response.getFailedRows().isEmpty()) {
                List<Row> rowCollction = new ArrayList<Row>();
                for (BatchGetRowResponse.RowResult result : response.getSucceedRows()) {
                    if (result.getRow() != null) {
                        rowCollction.add(result.getRow());
                    }
                }
                if (rowCollction.size() > 0) {
                    rowIterators.add(rowCollction.iterator());
                }
            }
        }

        if (rowIterators.size() > 0) {
            iteratorIndex = 0;
            current = rowIterators.get(iteratorIndex);
        } else {
            iteratorIndex = 0;
            current = null;
        }
    }

    private BatchGetRowRequest buildBatchGet(List<TablestoreSplit.PkRange> pkRanges, String tableName) {
        BatchGetRowRequest request = new BatchGetRowRequest();
        MultiRowQueryCriteria criteria1 = new MultiRowQueryCriteria(tableName);
        criteria1.setMaxVersions(1);
        for (TablestoreSplit.PkRange range : pkRanges) {
            if (range.getEqual() != null) {
                criteria1.addRow(range.getEqual());
                request.addMultiRowQueryCriteria(criteria1);
            }
        }
        return request;
    }

    private Iterator<Row> generateIterator(SyncClientInterface client, PrimaryKey begin, PrimaryKey end, String tableName, List<String> requiredColumns) {
        RangeIteratorParameter rangeIteratorParameter = new RangeIteratorParameter(tableName);
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
        if (requiredColumns != null && !requiredColumns.isEmpty()) {
            return client.createBulkExportIterator(rangeIteratorParameter);
        } else {
            return client.createRangeIterator(rangeIteratorParameter);
        }
    }

    @Override
    public long getTotalCount() {
        return -1;
    }

    @Override
    public boolean hasNext() {
        if (current != null && (current.hasNext() || iteratorIndex < rowIterators.size() - 1)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Row next() {
        if (current == null) {
            return null;
        }

        if (!current.hasNext()) {
            while (!current.hasNext() && iteratorIndex < rowIterators.size() -1) {
                iteratorIndex++;
                current = rowIterators.get(iteratorIndex);
            }
        }
        return current.next();
    }

    @Override
    public void remove() {
        throw new ClientException("RowIterator do not support remove().");
    }
}
