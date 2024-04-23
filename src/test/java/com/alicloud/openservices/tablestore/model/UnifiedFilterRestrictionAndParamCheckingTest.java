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
     * 构造一个深度为1000的composite filter，测试GetRow操作。
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
     * 构造一个深度为1000的composite filter，测试GetRange操作。
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
     * 构造一个深度为1000的composite filter，测试BatchGetRow操作。
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
     * 构造一个深度优先刚好超过和刚好不超过Filter个数限制的composite filter，测试GetRow操作。
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
     * 构造一个深度优先刚好超过和刚好不超过Filter个数限制的composite filter，测试GetRange操作。
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
     * 构造一个深度优先刚好超过和刚好不超过Filter个数限制的composite filter，测试BatchGetRow操作。
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
     * 构造一个广度优先的超过Filter个数限制的单层composite filter，测试GetRow操作。
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
     * 构造一个广度优先的超过Filter个数限制的单层composite filter，测试GetRange操作。
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
     * 构造一个广度优先的超过Filter个数限制的单层composite filter，测试BatchGetRow操作。
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
     * 构造一个超过Filter个数限制的二叉树composite filter，测试GetRow操作。
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
     * 构造一个超过Filter个数限制的二叉树composite filter，测试GetRange操作。
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
     * 构造一个超过Filter个数限制的二叉树composite filter，测试BatchGetRow操作。
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
     * 构造一个广度优先的恰好超过和恰好不超过Filter个数限制的composite filter，测试GetRow操作。
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

            // 广度优先无法使用NOT，NOT只允许一个Filter
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
     * 构造一个广度优先的恰好超过和恰好不超过Filter个数限制的composite filter，测试GetRange操作。
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
     * 构造一个广度优先的恰好超过和恰好不超过Filter个数限制的composite filter，测试BatchGetRow操作。
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
     * 构造一个恰好超过和恰好不超过Filter个数上限的二叉树的composite filter，测试GetRow操作。
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
     * 构造一个恰好超过和恰好不超过Filter个数上限的二叉树的composite filter，测试GetRange操作。
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
     * 构造一个恰好超过和恰好不超过Filter个数上限的二叉树的composite filter，测试BatchGetRow操作。
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
     * 构造一个长度超过BINARY/STRING列值最大长度的filter，类型分别是BINARY/STRING，期望被拒绝。分别测试单个relation filter，以及一个小的composite filter。
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
     * 构造一个column类型与cell类型不同的filter，交叉测试cell类型和filter类型分别是BINARY/STRING/INTEGER/DOUBLE/BOOLEAN。分别测试当个relation filter以及一个小的composite filter。
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
     * 构造 CompositeColumnValueFilter 类型为 NOT，并且sub filter数量为2或9，期望出错。
     * 构造 CompositeColumnValueFilter 类型为 AND/OR，并且sub filter数量为1，期望出错。
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
     * 构造一个一层的composite filter，里面包含AND或者OR，含有9个relation filter，每个relational filter的列名不一样，分别为[C1, C2 ... C9],
     * 分别测试GetRow/BatchGetRow/GetRange 3种请求，包含128列，列名长度255，列名分别为[C1, C2, ... C128], 类型为STRING或者BINARY，
     * Column大小为2K，使该行总大小为256KB。期望正常返回。
     *
     * Public模型下单行的数据量最大为256KB。
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
