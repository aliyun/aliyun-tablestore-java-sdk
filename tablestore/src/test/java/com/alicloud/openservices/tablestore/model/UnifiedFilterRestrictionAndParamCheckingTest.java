package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.*;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.model.filter.ColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.google.gson.JsonSyntaxException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UnifiedFilterRestrictionAndParamCheckingTest extends BaseFT {
    private static Logger LOG = Logger.getLogger(UnifiedFilterAdvanceTest.class.getName());

    private static String tableName = "FilterRestrictionTest";
    
    private static SyncClientInterface ots;

    private static final int SECONDS_UNTIL_TABLE_READY = 10;

    private static final int MAX_FILTER_COUNT = 32;

    @BeforeClass
    public static void classBefore() throws JsonSyntaxException, IOException {
        ots = Utils.getOTSInstance();
    }

    @AfterClass
    public static void classAfter() {
        ots.shutdown();
    }

    @Before
    public void setup() throws Exception {
        OTSHelper.deleteAllTable(ots);

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("PK0", PrimaryKeyType.INTEGER);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);
    }

    public Filter makeFilterWithMaxDepth(int depth, CompositeColumnValueFilter.LogicOperator operator) {
        CompositeColumnValueFilter rootFilter = new CompositeColumnValueFilter(operator);
        CompositeColumnValueFilter lastFilter = rootFilter;
        int filterCount = 1;

        for (int i = 2; i < depth; i++) {
            if (operator != CompositeColumnValueFilter.LogicOperator.NOT) {
                lastFilter.addFilter(new SingleColumnValueFilter("ColumnA", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromString("A")));
                filterCount++;
            }

            CompositeColumnValueFilter tmp = new CompositeColumnValueFilter(operator);
            filterCount++;
            lastFilter.addFilter(tmp);
            lastFilter = tmp;
        }

        lastFilter.addFilter(new SingleColumnValueFilter("ColumnB", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromString("B")));
        filterCount++;

        if (operator != CompositeColumnValueFilter.LogicOperator.NOT) {
            lastFilter.addFilter(new SingleColumnValueFilter("ColumnC", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromString("C")));
            filterCount++;
        }

        if (operator != CompositeColumnValueFilter.LogicOperator.NOT) {
            for (int i = filterCount; i < MAX_FILTER_COUNT; i++) {
                lastFilter.addFilter(new SingleColumnValueFilter("ColumnD", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromBoolean(false)));
            }
        }
        return rootFilter;
    }

    public Filter makeFilterWithMaxBreadth(int maxCount, CompositeColumnValueFilter.LogicOperator operator) {
        CompositeColumnValueFilter rootFilter = new CompositeColumnValueFilter(operator);

        for (int i = 1; i < maxCount; i++) {
            rootFilter.addFilter(new SingleColumnValueFilter("Column_" + i, SingleColumnValueFilter.CompareOperator.GREATER_EQUAL, ColumnValue.fromString("A")));
        }

        return rootFilter;
    }

    public Filter createFilter(int filterNumber, int maxCount, CompositeColumnValueFilter.LogicOperator operator) {
        boolean hasLeftNode = (2 * filterNumber) <= maxCount;
        boolean hasRightNode = (2 * filterNumber + 1) <= maxCount;

        //System.out.println(filterNumber + " " + hasLeftNode + " " + hasRightNode + " " + maxCount);
        if (hasLeftNode || hasRightNode) {
            if (hasRightNode) {
                return new CompositeColumnValueFilter(operator);
            } else {
                // if only has left node, the operator should only be NOT
                return new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.NOT);
            }
        } else {
            return new SingleColumnValueFilter("Column1_" + filterNumber, SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromString("A"));
        }
    }

    public Filter makeFilterInBinaryForm(int maxCount, CompositeColumnValueFilter.LogicOperator operator) {
        if (maxCount <= 0) {
            throw new IllegalArgumentException("There must be at least one filter.");
        }

        if (maxCount == 1) {
            return new SingleColumnValueFilter("Column1", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromString("A"));
        }

        List<Filter> filters = new ArrayList<Filter>();
        filters.add(new CompositeColumnValueFilter(operator));

        for (int index = 0; index < maxCount; index++) {
            int filterNumber = index + 1;
            int leftFilterNumber = 2 * filterNumber;
            int rightFilterNumber = leftFilterNumber + 1;

            if (!(filters.get(index) instanceof CompositeColumnValueFilter)) {
                continue;
            }

            CompositeColumnValueFilter rootFilter = (CompositeColumnValueFilter)filters.get(index);
            // add left node
            if (leftFilterNumber <= maxCount) {
                Filter leftFilter = createFilter(leftFilterNumber, maxCount, operator);
                rootFilter.addFilter((ColumnValueFilter) leftFilter);
                filters.add(leftFilter);
            }

            // add right node
            if (rightFilterNumber <= maxCount) {
                Filter rightFilter = createFilter(rightFilterNumber, maxCount, operator);
                rootFilter.addFilter((ColumnValueFilter) rightFilter);
                filters.add(rightFilter);
            }
        }

        //printFilter(filters);

        return filters.get(0);
    }

    private void printFilter(List<Filter> filters) {
        for (Filter filter : filters) {
            if (filter instanceof CompositeColumnValueFilter) {
                System.out.println("CompositeColumnValueFilter " + ((CompositeColumnValueFilter) filter).getOperationType());
            } else {
                System.out.println("SingleColumnValueFilter");
            }
        }
    }

    /**
     * Construct a composite filter with a depth of 1000 to test the GetRow operation.
     */
    @Test
    public void testCompositeColumnValueFilter_ExceedMaxDepth_GetRow() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey = new PrimaryKey(pks);
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(primaryKey);

        // test OR
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeColumnValueFilter.LogicOperator.OR));
            criteria.setMaxVersions(1);
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeColumnValueFilter.LogicOperator.AND));
            criteria.setMaxVersions(1);
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeColumnValueFilter.LogicOperator.NOT));
            criteria.setMaxVersions(1);
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }
    }


    /**
     * Construct a composite filter with a depth of 1000 to test the GetRange operation.
     */
    @Test
    public void testCompositeColumnValueFilter_ExceedMaxDepth_GetRange() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey begin = new PrimaryKey(pks);
        pks.clear();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10)));
        PrimaryKey end = new PrimaryKey(pks);

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setExclusiveEndPrimaryKey(end);
         

        // test OR
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeColumnValueFilter.LogicOperator.OR));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeColumnValueFilter.LogicOperator.AND));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeColumnValueFilter.LogicOperator.NOT));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }
    }

    /**
     * Construct a composite filter with a depth of 1000 to test the BatchGetRow operation.
     */
    @Test
    public void testCompositeColumnValueFilter_ExceedMaxDepth_BatchGetRow() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey = new PrimaryKey(pks);

        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
         
        criteria.addRow(primaryKey);

        // test OR
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeColumnValueFilter.LogicOperator.OR));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeColumnValueFilter.LogicOperator.AND));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeColumnValueFilter.LogicOperator.NOT));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }
    }

    /**
     * Construct a composite filter that just exceeds and just does not exceed the Filter limit in depth-first order, to test the GetRow operation.
     */
    @Test
    public void testCompositeColumnValueFilter_NotExceedMaxDepth_GetRow() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey = new PrimaryKey(pks);
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(primaryKey);
         

        // test OR
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(20, CompositeColumnValueFilter.LogicOperator.OR));
                criteria.setMaxVersions(1);
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(5, CompositeColumnValueFilter.LogicOperator.OR));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }

        // test AND
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(20, CompositeColumnValueFilter.LogicOperator.AND));
                criteria.setMaxVersions(1);
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(5, CompositeColumnValueFilter.LogicOperator.AND));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(33, CompositeColumnValueFilter.LogicOperator.NOT));
                criteria.setMaxVersions(1);
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(30, CompositeColumnValueFilter.LogicOperator.NOT));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }
    }

    /**
     * Construct a composite filter that just exceeds and just does not exceed the Filter limit in depth-first order, to test the GetRange operation.
     */
    @Test
    public void testCompositeColumnValueFilter_NotExceedMaxDepth_GetRange() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey begin = new PrimaryKey(pks);
        pks.clear();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10)));
        PrimaryKey end = new PrimaryKey(pks);

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setExclusiveEndPrimaryKey(end);
         

        // test OR
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(32, CompositeColumnValueFilter.LogicOperator.OR));
                criteria.setMaxVersions(1);
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(5, CompositeColumnValueFilter.LogicOperator.OR));
            criteria.setMaxVersions(1);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }

        // test AND
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(30, CompositeColumnValueFilter.LogicOperator.AND));
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(5, CompositeColumnValueFilter.LogicOperator.AND));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(33, CompositeColumnValueFilter.LogicOperator.NOT));
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(10, CompositeColumnValueFilter.LogicOperator.NOT));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }
    }

    /**
     * Construct a composite filter with depth-first traversal that just exceeds and just does not exceed the Filter number limit, to test the BatchGetRow operation.
     */
    @Test
    public void testCompositeColumnValueFilter_NotExceedMaxDepth_BatchGetRow() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey1 = new PrimaryKey(pks);
        pks.clear();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10)));
        PrimaryKey primaryKey2 = new PrimaryKey(pks);

        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
         
        criteria.addRow(primaryKey1);
        criteria.addRow(primaryKey2);
        // test AND
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(32, CompositeColumnValueFilter.LogicOperator.AND));
                criteria.setMaxVersions(1);
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                BatchGetRowResponse Response = ots.batchGetRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(5, CompositeColumnValueFilter.LogicOperator.AND));
            criteria.setMaxVersions(1);
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResponse Response = ots.batchGetRow(request);
            assertEquals(Response.getFailedRows().size(), 0);
        }

        // test OR
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(32, CompositeColumnValueFilter.LogicOperator.OR));
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                BatchGetRowResponse Response = ots.batchGetRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(5, CompositeColumnValueFilter.LogicOperator.OR));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResponse Response = ots.batchGetRow(request);
            assertEquals(Response.getFailedRows().size(), 0);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(33, CompositeColumnValueFilter.LogicOperator.NOT));
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                BatchGetRowResponse Response = ots.batchGetRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(10, CompositeColumnValueFilter.LogicOperator.NOT));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResponse Response = ots.batchGetRow(request);
            assertEquals(Response.getFailedRows().size(), 0);
        }
    }

    /**
     * Construct a breadth-first single-layer composite filter that exceeds the Filter count limit to test the GetRow operation.
     */
    @Test
    public void testCompositeColumnValueFilter_ExceedMaxBreadth_GetRow() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey = new PrimaryKey(pks);
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(primaryKey);

        // test OR
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeColumnValueFilter.LogicOperator.OR));
            criteria.setMaxVersions(1);
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeColumnValueFilter.LogicOperator.AND));
            criteria.setMaxVersions(1);
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeColumnValueFilter.LogicOperator.NOT));
            criteria.setMaxVersions(1);
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }
    }

    /**
     * Construct a breadth-first single-layer composite filter that exceeds the Filter limit to test the GetRange operation.
     */
    @Test
    public void testCompositeColumnValueFilter_ExceedMaxBreadth_GetRange() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey begin = new PrimaryKey(pks);
        pks.clear();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10)));
        PrimaryKey end = new PrimaryKey(pks);

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setExclusiveEndPrimaryKey(end);
         

        // test OR
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeColumnValueFilter.LogicOperator.OR));
            criteria.setMaxVersions(1);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeColumnValueFilter.LogicOperator.AND));
            criteria.setMaxVersions(1);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeColumnValueFilter.LogicOperator.NOT));
            criteria.setMaxVersions(1);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }
    }

    /**
     * Construct a breadth-first single-layer composite filter that exceeds the Filter count limit to test the BatchGetRow operation.
     */
    @Test
    public void testCompositeColumnValueFilter_ExceedMaxBreadth_BatchGetRow() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey = new PrimaryKey(pks);

        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
         
        criteria.addRow(primaryKey);

        // test OR
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeColumnValueFilter.LogicOperator.OR));
            criteria.setMaxVersions(1);
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeColumnValueFilter.LogicOperator.AND));
            criteria.setMaxVersions(1);
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeColumnValueFilter.LogicOperator.NOT));
            criteria.setMaxVersions(1);
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }
    }

    /**
     * Construct a binary tree composite filter that exceeds the Filter count limit to test the GetRow operation.
     */
    @Test
    public void testCompositeColumnValueFilterInBinaryTree_ExceedMaxCount_GetRow() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey = new PrimaryKey(pks);
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(primaryKey);
         

        // test OR
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeColumnValueFilter.LogicOperator.OR));
            criteria.setMaxVersions(1);
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeColumnValueFilter.LogicOperator.AND));
            criteria.setMaxVersions(1);
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeColumnValueFilter.LogicOperator.NOT));
            criteria.setMaxVersions(1);
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }
    }

    /**
     * Constructs a binary tree composite filter that exceeds the Filter count limit to test the GetRange operation.
     */
    @Test
    public void testCompositeColumnValueFilterInBinaryTree_ExceedMaxCount_GetRange() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey begin = new PrimaryKey(pks);
        pks.clear();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10)));
        PrimaryKey end = new PrimaryKey(pks);

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setExclusiveEndPrimaryKey(end);
         

        // test OR
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeColumnValueFilter.LogicOperator.OR));
            criteria.setMaxVersions(1);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeColumnValueFilter.LogicOperator.AND));
            criteria.setMaxVersions(1);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeColumnValueFilter.LogicOperator.NOT));
            criteria.setMaxVersions(1);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }
    }

    /**
     * Construct a binary tree composite filter that exceeds the Filter count limit to test the BatchGetRow operation.
     */
    @Test
    public void testCompositeColumnValueFilterInBinaryTreeExceedMaxCount_BatchGetRow() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey = new PrimaryKey(pks);

        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
         
        criteria.addRow(primaryKey);

        // test OR
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeColumnValueFilter.LogicOperator.OR));
            criteria.setMaxVersions(1);
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeColumnValueFilter.LogicOperator.AND));
            criteria.setMaxVersions(1);
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeColumnValueFilter.LogicOperator.NOT));
            criteria.setMaxVersions(1);
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
        }
    }

    /**
     * Construct a breadth-first composite filter that is exactly over and exactly not over the filter count limit, for testing the GetRow operation.
     */
    @Test
    public void testCompositeColumnValueFilter_NotExceedMaxBreadth_GetRow() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey = new PrimaryKey(pks);
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(primaryKey);
         

        // test OR
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(33, CompositeColumnValueFilter.LogicOperator.OR));
                criteria.setMaxVersions(1);
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeColumnValueFilter.LogicOperator.OR));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(33, CompositeColumnValueFilter.LogicOperator.NOT));
                criteria.setMaxVersions(1);
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }

            // Breadth-first search cannot use NOT, as NOT is only allowed to have one filter.
        }

        // test AND
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(33, CompositeColumnValueFilter.LogicOperator.AND));
                criteria.setMaxVersions(1);
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeColumnValueFilter.LogicOperator.AND));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }
    }

    /**
     * Construct a breadth-first composite filter that is exactly over and exactly not over the filter count limit, to test the GetRange operation.
     */
    @Test
    public void testCompositeColumnValueFilter_NotExceedMaxBreadth_GetRange() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey begin = new PrimaryKey(pks);
        pks.clear();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10)));
        PrimaryKey end = new PrimaryKey(pks);

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setExclusiveEndPrimaryKey(end);
         

        // test OR
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(33, CompositeColumnValueFilter.LogicOperator.OR));
                criteria.setMaxVersions(1);
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);

            }

            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeColumnValueFilter.LogicOperator.OR));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }

        // test AND
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(33, CompositeColumnValueFilter.LogicOperator.AND));
                criteria.setMaxVersions(1);
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeColumnValueFilter.LogicOperator.OR));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(33, CompositeColumnValueFilter.LogicOperator.NOT));
                criteria.setMaxVersions(1);
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeColumnValueFilter.LogicOperator.OR));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }
    }

    /**
     * Construct a breadth-first composite filter that is exactly over and exactly under the filter count limit to test the BatchGetRow operation.
     */
    @Test
    public void testCompositeColumnValueFilter_NotExceedMaxBreadth_BatchGetRow() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey = new PrimaryKey(pks);

        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
         
        criteria.addRow(primaryKey);

        // test OR
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(33, CompositeColumnValueFilter.LogicOperator.OR));
                criteria.setMaxVersions(1);
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                ots.batchGetRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeColumnValueFilter.LogicOperator.AND));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResponse Response = ots.batchGetRow(request);
            assertEquals(Response.getFailedRows().size(), 0);
        }

        // test AND
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(33, CompositeColumnValueFilter.LogicOperator.AND));
                criteria.setMaxVersions(1);
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                ots.batchGetRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeColumnValueFilter.LogicOperator.AND));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResponse Response = ots.batchGetRow(request);
            assertEquals(Response.getFailedRows().size(), 0);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(33, CompositeColumnValueFilter.LogicOperator.NOT));
                criteria.setMaxVersions(1);
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                ots.batchGetRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeColumnValueFilter.LogicOperator.AND));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResponse Response = ots.batchGetRow(request);
            assertEquals(Response.getFailedRows().size(), 0);
        }
    }

    /**
     * Construct a composite filter in the form of a binary tree that exactly exceeds and does not exceed the upper limit of the number of filters, to test the GetRow operation.
     */
    @Test
    public void testCompositeColumnValueFilterInBinaryTree_NotExceedMaxCount_GetRow() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey = new PrimaryKey(pks);
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(primaryKey);
         

        // test OR
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(33, CompositeColumnValueFilter.LogicOperator.OR));
                criteria.setMaxVersions(1);
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeColumnValueFilter.LogicOperator.OR));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }

        // test AND
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(33, CompositeColumnValueFilter.LogicOperator.AND));
                criteria.setMaxVersions(1);
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeColumnValueFilter.LogicOperator.OR));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(33, CompositeColumnValueFilter.LogicOperator.NOT));
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeColumnValueFilter.LogicOperator.OR));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }
    }

    /**
     * Construct a composite filter in the form of a binary tree that has exactly one more and one less than the upper limit of the Filter count, to test the GetRange operation.
     */
    @Test
    public void testCompositeColumnValueFilterInBinaryTree_NotExceedMaxCount_GetRange() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey begin = new PrimaryKey(pks);
        pks.clear();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10)));
        PrimaryKey end = new PrimaryKey(pks);

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setExclusiveEndPrimaryKey(end);
         

        // test OR
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(33, CompositeColumnValueFilter.LogicOperator.OR));
                criteria.setMaxVersions(1);
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }
            criteria.setFilter(makeFilterInBinaryForm(10, CompositeColumnValueFilter.LogicOperator.OR));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }

        // test AND
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(33, CompositeColumnValueFilter.LogicOperator.AND));
                criteria.setMaxVersions(1);
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }
            criteria.setFilter(makeFilterInBinaryForm(10, CompositeColumnValueFilter.LogicOperator.AND));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }
    }

    /**
     * Constructs a composite filter in the form of a binary tree that has exactly one more and one less than the filter limit, to test the BatchGetRow operation.
     */
    @Test
    public void testCompositeColumnValueFilterInBinaryTree_NotExceedMaxCount_BatchGetRow() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey = new PrimaryKey(pks);

        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
         
        criteria.addRow(primaryKey);

        // test OR
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(33, CompositeColumnValueFilter.LogicOperator.OR));
                criteria.setMaxVersions(1);
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                ots.batchGetRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }
            criteria.setFilter(makeFilterInBinaryForm(10, CompositeColumnValueFilter.LogicOperator.OR));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResponse Response = ots.batchGetRow(request);
            assertEquals(Response.getFailedRows().size(), 0);
        }
        // test AND
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(33, CompositeColumnValueFilter.LogicOperator.AND));
                criteria.setMaxVersions(1);
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                ots.batchGetRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }
            criteria.setFilter(makeFilterInBinaryForm(10, CompositeColumnValueFilter.LogicOperator.AND));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResponse Response = ots.batchGetRow(request);
            assertEquals(Response.getFailedRows().size(), 0);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(33, CompositeColumnValueFilter.LogicOperator.NOT));
                criteria.setMaxVersions(1);
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                ots.batchGetRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(ErrorCode.INVALID_PARAMETER, "The count of filter exceeds the max: 32.", 400, e);
            }
        }
    }

    /**
     * Construct a filter with a length exceeding the maximum length of BINARY/STRING column values, with types being BINARY/STRING respectively, and expect it to be rejected. Test a single relation filter as well as a small composite filter separately.
     */
    @Test
    public void testValueExceedMaxLength() {
        // STRING
        {
            ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
            pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
            PrimaryKey primaryKey = new PrimaryKey(pks);
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
            criteria.setPrimaryKey(primaryKey);
            criteria.setMaxVersions(1);
             
            ColumnValue v = ColumnValue.fromString(NewString(2 * 1024 * 1024 + 1, 'a'));
            SingleColumnValueFilter singleFilter = new SingleColumnValueFilter("Column", SingleColumnValueFilter.CompareOperator.EQUAL, v);
            CompositeColumnValueFilter compositeFilter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.OR);
            compositeFilter.addFilter(singleFilter);
            compositeFilter.addFilter(new SingleColumnValueFilter("Column1", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromBoolean(false)));

            TableStoreException lengthExceedExp = new TableStoreException(
                    "The length of attribute column: 'Column' exceeds the MaxLength:2097152 with CurrentLength:2097153.",
                    null, "OTSParameterInvalid", "", 400);
            runAndExpectFail(singleFilter, lengthExceedExp);
            runAndExpectFail(compositeFilter, lengthExceedExp);
        }

        // BINARY
        {
            ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
            pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
            PrimaryKey primaryKey = new PrimaryKey(pks);
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
            criteria.setMaxVersions(1);
            criteria.setPrimaryKey(primaryKey);
             
            ColumnValue v = ColumnValue.fromBinary(new byte[2 * 1024 * 1024 + 1]);
            SingleColumnValueFilter singleFilter = new SingleColumnValueFilter("Column", SingleColumnValueFilter.CompareOperator.EQUAL, v);
            CompositeColumnValueFilter compositeFilter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.OR);
            compositeFilter.addFilter(singleFilter);
            compositeFilter.addFilter(new SingleColumnValueFilter("Column1", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromBoolean(false)));

            TableStoreException lengthExceedExp = new TableStoreException(
                    "The length of attribute column: 'Column' exceeds the MaxLength:2097152 with CurrentLength:2097153.",
                    null, "OTSParameterInvalid", "", 400);
            runAndExpectFail(singleFilter, lengthExceedExp);
            runAndExpectFail(compositeFilter, lengthExceedExp);
        }
    }

    /**
     * Construct a filter with a different column type and cell type, for cross-testing when the cell type and filter type are BINARY/STRING/INTEGER/DOUBLE/BOOLEAN respectively. Test individual relation filters as well as a small composite filter separately.
     */
    @Test
    public void testCompareWithDifferentType() {
        PutRowRequest pr = new PutRowRequest();

        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey = new PrimaryKey(pks);
        RowPutChange rowChange = new RowPutChange(tableName);
        rowChange.setPrimaryKey(primaryKey);
        String[] names = new String[]{"col_int", "col_string", "col_double", "col_boolean", "col_byte"};
        ColumnValue[] values = new ColumnValue[] {ColumnValue.fromLong(0), ColumnValue.fromString("a"),
                ColumnValue.fromDouble(1.0), ColumnValue.fromBoolean(true), ColumnValue.fromBinary(new byte[]{0})};

        for (int i = 0; i < names.length; i++) {
            rowChange.addColumn(names[i], values[i]);
        }
        pr.setRowChange(rowChange);
        ots.putRow(pr);

        for (int i = 0; i < names.length; i++) {
            for (int j = 0; j < values.length; j++) {
                if (i == j) {
                    continue;
                }

                SingleColumnValueFilter singleFilter = new SingleColumnValueFilter(names[i], SingleColumnValueFilter.CompareOperator.EQUAL, values[j]);
                CompositeColumnValueFilter compositeFilter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.AND);
                compositeFilter.addFilter(singleFilter);
                compositeFilter.addFilter(new SingleColumnValueFilter("CC", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromBoolean(false)));

                SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName);
                rowQueryCriteria.setPrimaryKey(primaryKey);
                rowQueryCriteria.setFilter(singleFilter);
                rowQueryCriteria.setMaxVersions(1);
                GetRowRequest getRowRequest = new GetRowRequest(rowQueryCriteria);
                GetRowResponse result = ots.getRow(getRowRequest);
                Row resultRow = result.getRow();
                assertEquals(resultRow, null);
            }
        }
    }

    /**
     * Construct a CompositeColumnValueFilter of type NOT with the number of sub-filters being 2 or 9, expecting an error.
     * Construct a CompositeColumnValueFilter of type AND/OR with the number of sub-filters being 1, expecting an error.
     */
    @Test
    public void testLogicOperatorRestriction() {
        CompositeColumnValueFilter orFilter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.OR);
        CompositeColumnValueFilter andFilter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.AND);
        CompositeColumnValueFilter notFilter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.NOT);

        SingleColumnValueFilter singleFilter = new SingleColumnValueFilter("Column", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromBoolean(false));
        orFilter.addFilter(singleFilter);
        andFilter.addFilter(singleFilter);

        TableStoreException binaryExp = new TableStoreException("Invalid AND/OR operator: the number of sub-filters must be more than 1.", null, "OTSParameterInvalid", "", 400);
        TableStoreException notExp = new TableStoreException("Invalid NOT operator: the number of sub-filters must be 1.", null, "OTSParameterInvalid", "", 400);
        runAndExpectFail(orFilter, binaryExp);
        runAndExpectFail(andFilter, binaryExp);

        for (int i = 0; i < 2; i++) {
            notFilter.addFilter(singleFilter);
        }
        runAndExpectFail(notFilter, notExp);

        notFilter.clear();
        for (int i = 0; i < 9; i++) {
            notFilter.addFilter(singleFilter);
        }
        runAndExpectFail(notFilter, notExp);
    }

    private void runAndExpectFail(Filter filter, TableStoreException exp) {
        // GetRow
        {
            ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
            pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
            PrimaryKey primaryKey = new PrimaryKey(pks);
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
            criteria.setPrimaryKey(primaryKey);
             
            criteria.setFilter(filter);
            criteria.setMaxVersions(1);
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);

            try {
                ots.getRow(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(exp.getErrorCode(), exp.getMessage(), exp.getHttpStatus(), e);
            }
        }

        // GetRange
        {
            ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
            pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
            PrimaryKey begin = new PrimaryKey(pks);
            pks.clear();
            pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10)));
            PrimaryKey end = new PrimaryKey(pks);

            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(begin);
            criteria.setExclusiveEndPrimaryKey(end);
            criteria.setFilter(filter);
            criteria.setMaxVersions(1);
             

            try {
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (TableStoreException e) {
                assertTableStoreException(exp.getErrorCode(), exp.getMessage(), exp.getHttpStatus(), e);
            }
        }

        // BatchGetRow
        {
            ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
            pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
            PrimaryKey primaryKey = new PrimaryKey(pks);
            MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
             
            criteria.addRow(primaryKey);
            criteria.setFilter(filter);
            criteria.setMaxVersions(1);

            try {
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                BatchGetRowResponse Response = ots.batchGetRow(request);
                if (!Response.isAllSucceed()) {
                    assertEquals(Response.getFailedRows().size(), 1);
                    assertEquals(Response.getFailedRows().get(0).getError().getCode(), exp.getErrorCode());
                    assertEquals(Response.getFailedRows().get(0).getError().getMessage(), exp.getMessage());
                } else {
                    fail();
                }
            } catch (TableStoreException e) {
                assertTableStoreException(exp.getErrorCode(), exp.getMessage(), exp.getHttpStatus(), e);
            }
        }
    }

    /**
     * Constructs a one-layer composite filter containing AND or OR, with 9 relational filters, each having different column names [C1, C2, ..., C9].
     * Tests three types of requests: GetRow, BatchGetRow, and GetRange. Each request involves 128 columns with column names of length 255, named [C1, C2, ..., C128], 
     * with types being either STRING or BINARY. The size of each column is 2K, making the total row size 256KB. Expected to return normally.
     *
     * In the Public model, the maximum data size for a single row is 256KB.
     */
    @Test
    public void testColumnNameMaxLength() {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey = new PrimaryKey(pks);
        ColumnValue columnValue = ColumnValue.fromString(NewString(2 * 896, 'c'));

        for (int i = 0; i < 16; i++) {
            UpdateRowRequest pr = new UpdateRowRequest();
            RowUpdateChange rowChange = new RowUpdateChange(tableName);
            rowChange.setPrimaryKey(primaryKey);
            for (int j = 0; j < 8; j++) {
                rowChange.put(String.format("C%0254d", i * 8 + j), columnValue);
            }

            pr.setRowChange(rowChange);
            ots.updateRow(pr);
        }

        {
            CompositeColumnValueFilter compositeFilter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.OR);
            for (int i = 0; i < 9; i++) {
                compositeFilter.addFilter(new SingleColumnValueFilter(String.format("C%0254d", i), SingleColumnValueFilter.CompareOperator.EQUAL, columnValue));
            }
            testFilterWithMaxNameLength(compositeFilter);
        }
        {
            CompositeColumnValueFilter compositeFilter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.AND);
            for (int i = 0; i < 9; i++) {
                compositeFilter.addFilter(new SingleColumnValueFilter(String.format("C%0254d", i), SingleColumnValueFilter.CompareOperator.EQUAL, columnValue));
            }
            testFilterWithMaxNameLength(compositeFilter);
        }
    }

    private void testFilterWithMaxNameLength(CompositeColumnValueFilter compositeFilter) {
        ArrayList<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
        PrimaryKey primaryKey = new PrimaryKey(pks);
        ColumnValue columnValue = ColumnValue.fromString(NewString(2 * 896, 'c'));

        List<Row> rows = new ArrayList<Row>();

        // get row
        {

            GetRowRequest request = new GetRowRequest();
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
            criteria.setPrimaryKey(primaryKey);
             
            criteria.setFilter(compositeFilter);
            criteria.setMaxVersions(1);
            request.setRowQueryCriteria(criteria);

            GetRowResponse Response = ots.getRow(request);
            rows.add(Response.getRow());
        }

        // get range
        {
            pks.clear();
            pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
            PrimaryKey begin = new PrimaryKey(pks);
            pks.clear();
            pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10)));
            PrimaryKey end = new PrimaryKey(pks);

            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(begin);
            criteria.setExclusiveEndPrimaryKey(end);
             
            criteria.setFilter(compositeFilter);
            criteria.setMaxVersions(1);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            GetRangeResponse Response = ots.getRange(request);
            assertEquals(Response.getRows().size(), 1);
            rows.add(Response.getRows().get(0));
        }

        // batch get row
        {
            MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
             
            criteria.addRow(primaryKey);
            criteria.setFilter(compositeFilter);
            criteria.setMaxVersions(1);

            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);

            BatchGetRowResponse Response = ots.batchGetRow(request);
            assertTrue(Response.isAllSucceed());
            assertEquals(Response.getSucceedRows().size(), 1);
            rows.add(Response.getSucceedRows().get(0).getRow());
        }

        for (Row row : rows) {
            pks.clear();
            pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0)));
            PrimaryKey pk = new PrimaryKey(pks);
            Column[] cols = new Column[128];
            for (int j = 0; j < 128; j++) {
                cols[j] = new Column(String.format("C%0254d", j), ColumnValue.fromString(NewString(2 * 896, 'c')));
            }
            Row expect = new Row(pk, cols);
            checkRowNoTimestamp(expect, row);
        }
    }

    private String NewString(int length, char a) {
        char[] cs = new char[length];
        for (int i = 0; i < cs.length; i++) {
            cs[i] = a;
        }
        return new String(cs);
    }
}
