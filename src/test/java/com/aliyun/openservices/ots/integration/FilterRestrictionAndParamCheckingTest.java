package com.aliyun.openservices.ots.integration;

import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.OTSErrorCode;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.common.BaseFT;
import com.aliyun.openservices.ots.common.OTSHelper;
import com.aliyun.openservices.ots.common.Utils;
import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.model.condition.ColumnCondition;
import com.aliyun.openservices.ots.model.condition.CompositeCondition;
import com.aliyun.openservices.ots.model.condition.RelationalCondition;
import com.aliyun.openservices.ots.utils.ServiceSettings;
import com.aliyun.openservices.ots.utils.TestUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FilterRestrictionAndParamCheckingTest extends BaseFT {
    private static Logger LOG = Logger.getLogger(FilterAdvanceTest.class.getName());

    private static String tableName = TestUtil.newTableName("FilterRestrictionTest");

    private static final OTS ots = OTSClientFactory.createOTSClient(ServiceSettings.load());

    private static final int SECONDS_UNTIL_TABLE_READY = 10;

    private static final int MAX_FILTER_COUNT = 10;

    @AfterClass
    public static void classAfter() {
        ots.shutdown();
    }

    @Before
    public void setup() throws Exception {
        OTSHelper.deleteAllTable(ots);
        LOG.info("Instance: " + ServiceSettings.load().getOTSInstanceName());

        ListTableResult r = ots.listTable();

        for (String table: r.getTableNames()) {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest(table);
            ots.deleteTable(deleteTableRequest);
            LOG.info("Delete table: " + table);

            Thread.sleep(1000);
        }

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("PK0", PrimaryKeyType.INTEGER);

        OTSHelper.createTable(ots, tableMeta);
        Utils.sleepSeconds(SECONDS_UNTIL_TABLE_READY);
    }

    public ColumnCondition makeFilterWithMaxDepth(int depth, CompositeCondition.LogicOperator operator) {
        CompositeCondition rootFilter = new CompositeCondition(operator);
        CompositeCondition lastFilter = rootFilter;
        int filterCount = 1;

        for (int i = 2; i < depth; i++) {
            if (operator != CompositeCondition.LogicOperator.NOT) {
                lastFilter.addCondition(new RelationalCondition("ColumnA", RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("A")));
                filterCount++;
            }

            CompositeCondition tmp = new CompositeCondition(operator);
            filterCount++;
            lastFilter.addCondition(tmp);
            lastFilter = tmp;
        }

        lastFilter.addCondition(new RelationalCondition("ColumnB", RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("B")));
        filterCount++;

        if (operator != CompositeCondition.LogicOperator.NOT) {
            lastFilter.addCondition(new RelationalCondition("ColumnC", RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("C")));
            filterCount++;
        }

        for (int i = filterCount; i < MAX_FILTER_COUNT; i++) {
            lastFilter.addCondition(new RelationalCondition("ColumnD", RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromBoolean(false)));
        }
        return rootFilter;
    }

    public ColumnCondition makeFilterWithMaxBreadth(int maxCount, CompositeCondition.LogicOperator operator) {
        CompositeCondition rootFilter = new CompositeCondition(operator);

        for (int i = 1; i < maxCount; i++) {
            rootFilter.addCondition(new RelationalCondition("Column_" + i, RelationalCondition.CompareOperator.GREATER_EQUAL, ColumnValue.fromString("A")));
        }

        return rootFilter;
    }

    public ColumnCondition createFilter(int filterNumber, int maxCount, CompositeCondition.LogicOperator operator) {
        boolean hasLeftNode = (2 * filterNumber) <= maxCount;
        boolean hasRightNode = (2 * filterNumber + 1) <= maxCount;

        //System.out.println(filterNumber + " " + hasLeftNode + " " + hasRightNode + " " + maxCount);
        if (hasLeftNode || hasRightNode) {
            if (hasRightNode) {
                return new CompositeCondition(operator);
            } else {
                // if only has left node, the operator should only be NOT
                return new CompositeCondition(CompositeCondition.LogicOperator.NOT);
            }
        } else {
            return new RelationalCondition("Column1_" + filterNumber, RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("A"));
        }
    }

    public ColumnCondition makeFilterInBinaryForm(int maxCount, CompositeCondition.LogicOperator operator) {
        if (maxCount <= 0) {
            throw new IllegalArgumentException("There must be at least one filter.");
        }

        if (maxCount == 1) {
            return new RelationalCondition("Column1", RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromString("A"));
        }

        List<ColumnCondition> filters = new ArrayList<ColumnCondition>();
        filters.add(new CompositeCondition(operator));

        for (int index = 0; index < maxCount; index++) {
            int filterNumber = index + 1;
            int leftFilterNumber = 2 * filterNumber;
            int rightFilterNumber = leftFilterNumber + 1;

            if (!(filters.get(index) instanceof CompositeCondition)) {
                continue;
            }

            CompositeCondition rootFilter = (CompositeCondition)filters.get(index);
            // add left node
            if (leftFilterNumber <= maxCount) {
                ColumnCondition leftFilter = createFilter(leftFilterNumber, maxCount, operator);
                rootFilter.addCondition(leftFilter);
                filters.add(leftFilter);
            }

            // add right node
            if (rightFilterNumber <= maxCount) {
                ColumnCondition rightFilter = createFilter(rightFilterNumber, maxCount, operator);
                rootFilter.addCondition(rightFilter);
                filters.add(rightFilter);
            }
        }

        //printFilter(filters);

        return filters.get(0);
    }

    private void printFilter(List<ColumnCondition> filters) {
        for (ColumnCondition filter : filters) {
            if (filter instanceof CompositeCondition) {
                System.out.println("CompositeCondition " + ((CompositeCondition) filter).getOperationType());
            } else {
                System.out.println("RelationalCondition");
            }
        }
    }

    /**
     * 构造一个深度为1000的composite filter，测试GetRow操作。
     */
    @Test
    public void testCompositeCondition_ExceedMaxDepth_GetRow() {
        LOG.info("Start testCompositeCondition_ExceedMaxDepth_GetRow");

        RowPrimaryKey primaryKey =
                new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(primaryKey);

        // test OR
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeCondition.LogicOperator.OR));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeCondition.LogicOperator.AND));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeCondition.LogicOperator.NOT));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }
    }


    /**
     * 构造一个深度为1000的composite filter，测试GetRange操作。
     */
    @Test
    public void testCompositeCondition_ExceedMaxDepth_GetRange() {
        LOG.info("Start testCompositeCondition_ExceedMaxDepth_GetRange");

        RowPrimaryKey begin = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        RowPrimaryKey end = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10));

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setExclusiveEndPrimaryKey(end);
         

        // test OR
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeCondition.LogicOperator.OR));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeCondition.LogicOperator.AND));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeCondition.LogicOperator.NOT));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }
    }

    /**
     * 构造一个深度为1000的composite filter，测试BatchGetRow操作。
     */
    @Test
    public void testCompositeCondition_ExceedMaxDepth_BatchGetRow() {
        LOG.info("Start testCompositeCondition_ExceedMaxDepth_BatchGetRow");

        RowPrimaryKey primaryKey = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));

        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
         
        criteria.addRow(primaryKey);

        // test OR
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeCondition.LogicOperator.OR));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeCondition.LogicOperator.AND));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterWithMaxDepth(1000, CompositeCondition.LogicOperator.NOT));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }
    }

    /**
     * 构造一个深度优先刚好超过和刚好不超过Filter个数限制的composite filter，测试GetRow操作。
     */
    @Test
    public void testCompositeCondition_NotExceedMaxDepth_GetRow() {
        LOG.info("Start testCompositeCondition_NotExceedMaxDepth_GetRow");

        RowPrimaryKey primaryKey = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(primaryKey);
         

        // test OR
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(6, CompositeCondition.LogicOperator.OR));
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(5, CompositeCondition.LogicOperator.OR));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }

        // test AND
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(6, CompositeCondition.LogicOperator.AND));
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(5, CompositeCondition.LogicOperator.AND));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(11, CompositeCondition.LogicOperator.NOT));
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(10, CompositeCondition.LogicOperator.NOT));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }
    }

    /**
     * 构造一个深度优先刚好超过和刚好不超过Filter个数限制的composite filter，测试GetRange操作。
     */
    @Test
    public void testCompositeCondition_NotExceedMaxDepth_GetRange() {
        LOG.info("Start testCompositeCondition_NotExceedMaxDepth_GetRange");

        RowPrimaryKey begin = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        RowPrimaryKey end = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10));

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setExclusiveEndPrimaryKey(end);
         

        // test OR
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(6, CompositeCondition.LogicOperator.OR));
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(5, CompositeCondition.LogicOperator.OR));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }

        // test AND
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(6, CompositeCondition.LogicOperator.AND));
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(5, CompositeCondition.LogicOperator.AND));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(11, CompositeCondition.LogicOperator.NOT));
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(10, CompositeCondition.LogicOperator.NOT));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }
    }

    /**
     * 构造一个深度优先刚好超过和刚好不超过Filter个数限制的composite filter，测试BatchGetRow操作。
     */
    @Test
    public void testCompositeCondition_NotExceedMaxDepth_BatchGetRow() {
        LOG.info("Start testCompositeCondition_NotExceedMaxDepth_BatchGetRow");

        RowPrimaryKey primaryKey1 = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        RowPrimaryKey primaryKey2 = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10));

        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
         
        criteria.addRow(primaryKey1);
        criteria.addRow(primaryKey2);
        // test AND
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(6, CompositeCondition.LogicOperator.AND));
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                BatchGetRowResult result = ots.batchGetRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(5, CompositeCondition.LogicOperator.AND));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResult result = ots.batchGetRow(request);
            assertEquals(result.getFailedRows().size(), 0);
        }

        // test OR
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(6, CompositeCondition.LogicOperator.OR));
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                BatchGetRowResult result = ots.batchGetRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(5, CompositeCondition.LogicOperator.OR));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResult result = ots.batchGetRow(request);
            assertEquals(result.getFailedRows().size(), 0);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterWithMaxDepth(11, CompositeCondition.LogicOperator.NOT));
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                BatchGetRowResult result = ots.batchGetRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxDepth(10, CompositeCondition.LogicOperator.NOT));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResult result = ots.batchGetRow(request);
            assertEquals(result.getFailedRows().size(), 0);
        }
    }

    /**
     * 构造一个广度优先的超过Filter个数限制的单层composite filter，测试GetRow操作。
     */
    @Test
    public void testCompositeCondition_ExceedMaxBreadth_GetRow() {
        LOG.info("Start testCompositeCondition_ExceedMaxBreadth_GetRow");

        RowPrimaryKey primaryKey = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(primaryKey);

        // test OR
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeCondition.LogicOperator.OR));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeCondition.LogicOperator.AND));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeCondition.LogicOperator.NOT));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }
    }

    /**
     * 构造一个广度优先的超过Filter个数限制的单层composite filter，测试GetRange操作。
     */
    @Test
    public void testCompositeCondition_ExceedMaxBreadth_GetRange() {
        LOG.info("Start testCompositeCondition_ExceedMaxBreadth_GetRange");

        RowPrimaryKey begin = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        RowPrimaryKey end = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10));

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setExclusiveEndPrimaryKey(end);
         

        // test OR
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeCondition.LogicOperator.OR));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeCondition.LogicOperator.AND));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeCondition.LogicOperator.NOT));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }
    }

    /**
     * 构造一个广度优先的超过Filter个数限制的单层composite filter，测试BatchGetRow操作。
     */
    @Test
    public void testCompositeCondition_ExceedMaxBreadth_BatchGetRow() {
        LOG.info("Start testCompositeCondition_ExceedMaxBreadth_BatchGetRow");

        RowPrimaryKey primaryKey = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));

        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
         
        criteria.addRow(primaryKey);

        // test OR
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeCondition.LogicOperator.OR));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeCondition.LogicOperator.AND));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterWithMaxBreadth(1000, CompositeCondition.LogicOperator.NOT));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }
    }

    /**
     * 构造一个超过Filter个数限制的二叉树composite filter，测试GetRow操作。
     */
    @Test
    public void testCompositeConditionInBinaryTree_ExceedMaxCount_GetRow() {
        LOG.info("Start testCompositeConditionInBinaryTree_ExceedMaxCount_GetRow");

        RowPrimaryKey primaryKey = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(primaryKey);
         

        // test OR
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeCondition.LogicOperator.OR));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeCondition.LogicOperator.AND));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeCondition.LogicOperator.NOT));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }
    }

    /**
     * 构造一个超过Filter个数限制的二叉树composite filter，测试GetRange操作。
     */
    @Test
    public void testCompositeConditionInBinaryTree_ExceedMaxCount_GetRange() {
        LOG.info("Start testCompositeConditionInBinaryTree_ExceedMaxCount_GetRange");

        RowPrimaryKey begin = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        RowPrimaryKey end = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10));

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setExclusiveEndPrimaryKey(end);
         

        // test OR
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeCondition.LogicOperator.OR));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeCondition.LogicOperator.AND));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeCondition.LogicOperator.NOT));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }
    }

    /**
     * 构造一个超过Filter个数限制的二叉树composite filter，测试BatchGetRow操作。
     */
    @Test
    public void testCompositeConditionInBinaryTreeExceedMaxCount_BatchGetRow() {
        LOG.info("Start testCompositeConditionInBinaryTreeExceedMaxCount_BatchGetRow");

        RowPrimaryKey primaryKey = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));

        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
         
        criteria.addRow(primaryKey);

        // test OR
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeCondition.LogicOperator.OR));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test AND
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeCondition.LogicOperator.AND));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }

        // test NOT
        try {
            criteria.setFilter(makeFilterInBinaryForm(1000, CompositeCondition.LogicOperator.NOT));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            ots.batchGetRow(request);
            fail();
        } catch (OTSException e) {
            assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
        }
    }

    /**
     * 构造一个广度优先的恰好超过和恰好不超过Filter个数限制的composite filter，测试GetRow操作。
     */
    @Test
    public void testCompositeCondition_NotExceedMaxBreadth_GetRow() {
        LOG.info("Start testCompositeCondition_NotExceedMaxBreadth_GetRow");

        RowPrimaryKey primaryKey = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(primaryKey);
         

        // test OR
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(11, CompositeCondition.LogicOperator.OR));
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeCondition.LogicOperator.OR));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(11, CompositeCondition.LogicOperator.NOT));
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }

            // 广度优先无法使用NOT，NOT只允许一个Filter
        }

        // test AND
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(11, CompositeCondition.LogicOperator.AND));
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }

            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeCondition.LogicOperator.AND));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }
    }

    /**
     * 构造一个广度优先的恰好超过和恰好不超过Filter个数限制的composite filter，测试GetRange操作。
     */
    @Test
    public void testCompositeCondition_NotExceedMaxBreadth_GetRange() {
        LOG.info("Start testCompositeCondition_NotExceedMaxBreadth_GetRange");

        RowPrimaryKey begin = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        RowPrimaryKey end = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10));

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setExclusiveEndPrimaryKey(end);
         

        // test OR
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(11, CompositeCondition.LogicOperator.OR));
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);

            }

            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeCondition.LogicOperator.OR));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }

        // test AND
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(11, CompositeCondition.LogicOperator.AND));
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeCondition.LogicOperator.OR));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(11, CompositeCondition.LogicOperator.NOT));
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeCondition.LogicOperator.OR));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }
    }

    /**
     * 构造一个广度优先的恰好超过和恰好不超过Filter个数限制的composite filter，测试BatchGetRow操作。
     */
    @Test
    public void testCompositeCondition_NotExceedMaxBreadth_BatchGetRow() {
        LOG.info("Start testCompositeCondition_NotExceedMaxBreadth_BatchGetRow");

        RowPrimaryKey primaryKey = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));

        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
         
        criteria.addRow(primaryKey);

        // test OR
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(11, CompositeCondition.LogicOperator.OR));
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                ots.batchGetRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeCondition.LogicOperator.AND));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResult result = ots.batchGetRow(request);
            assertEquals(result.getFailedRows().size(), 0);
        }

        // test AND
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(11, CompositeCondition.LogicOperator.AND));
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                ots.batchGetRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeCondition.LogicOperator.AND));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResult result = ots.batchGetRow(request);
            assertEquals(result.getFailedRows().size(), 0);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterWithMaxBreadth(11, CompositeCondition.LogicOperator.NOT));
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                ots.batchGetRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeCondition.LogicOperator.AND));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResult result = ots.batchGetRow(request);
            assertEquals(result.getFailedRows().size(), 0);
        }
    }

    /**
     * 构造一个恰好超过和恰好不超过Filter个数上限的二叉树的composite filter，测试GetRow操作。
     */
    @Test
    public void testCompositeConditionInBinaryTree_NotExceedMaxCount_GetRow() {
        LOG.info("Start testCompositeConditionInBinaryTree_NotExceedMaxCount_GetRow");

        RowPrimaryKey primaryKey = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
		criteria.setPrimaryKey(primaryKey);
         

        // test OR
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(11, CompositeCondition.LogicOperator.OR));
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeCondition.LogicOperator.OR));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }

        // test AND
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(11, CompositeCondition.LogicOperator.AND));
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeCondition.LogicOperator.OR));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(11, CompositeCondition.LogicOperator.NOT));
                GetRowRequest request = new GetRowRequest();
                request.setRowQueryCriteria(criteria);
                ots.getRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }
            criteria.setFilter(makeFilterWithMaxBreadth(10, CompositeCondition.LogicOperator.OR));
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);
            ots.getRow(request);
        }
    }

    /**
     * 构造一个恰好超过和恰好不超过Filter个数上限的二叉树的composite filter，测试GetRange操作。
     */
    @Test
    public void testCompositeConditionInBinaryTree_NotExceedMaxCount_GetRange() {
        LOG.info("Start testCompositeConditionInBinaryTree_NotExceedMaxCount_GetRange");

        RowPrimaryKey begin = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        RowPrimaryKey end = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10));

        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(begin);
        criteria.setExclusiveEndPrimaryKey(end);
         

        // test OR
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(11, CompositeCondition.LogicOperator.OR));
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }
            criteria.setFilter(makeFilterInBinaryForm(10, CompositeCondition.LogicOperator.OR));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }

        // test AND
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(11, CompositeCondition.LogicOperator.AND));
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }
            criteria.setFilter(makeFilterInBinaryForm(10, CompositeCondition.LogicOperator.AND));
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            ots.getRange(request);
        }
    }

    /**
     * 构造一个恰好超过和恰好不超过Filter个数上限的二叉树的composite filter，测试BatchGetRow操作。
     */
    @Test
    public void testCompositeConditionInBinaryTree_NotExceedMaxCount_BatchGetRow() {
        LOG.info("Start testCompositeConditionInBinaryTree_NotExceedMaxCount_BatchGetRow");

        RowPrimaryKey primaryKey = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));

        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
         
        criteria.addRow(primaryKey);

        // test OR
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(11, CompositeCondition.LogicOperator.OR));
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                ots.batchGetRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }
            criteria.setFilter(makeFilterInBinaryForm(10, CompositeCondition.LogicOperator.OR));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResult result = ots.batchGetRow(request);
            assertEquals(result.getFailedRows().size(), 0);
        }
        // test AND
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(11, CompositeCondition.LogicOperator.AND));
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                ots.batchGetRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }
            criteria.setFilter(makeFilterInBinaryForm(10, CompositeCondition.LogicOperator.AND));
            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);
            BatchGetRowResult result = ots.batchGetRow(request);
            assertEquals(result.getFailedRows().size(), 0);
        }

        // test NOT
        {
            try {
                criteria.setFilter(makeFilterInBinaryForm(11, CompositeCondition.LogicOperator.NOT));
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                ots.batchGetRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(OTSErrorCode.INVALID_PARAMETER, "The number of column conditions exceeds the limit: 10.", 400, e);
            }
        }
    }

    /**
     * 构造一个长度超过BINARY/STRING列值最大长度的filter，类型分别是BINARY/STRING，期望被拒绝。分别测试单个relation filter，以及一个小的composite filter。
     */
    @Test
    public void testValueExceedMaxLength() {
        LOG.info("Start testValueExceedMaxLength");

        // STRING
        {
            RowPrimaryKey primaryKey = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
		    criteria.setPrimaryKey(primaryKey);
             
            ColumnValue v = ColumnValue.fromString(NewString(64 * 1024 + 1, 'a'));
            RelationalCondition singleFilter = new RelationalCondition("Column", RelationalCondition.CompareOperator.EQUAL, v);
            CompositeCondition compositeFilter = new CompositeCondition(CompositeCondition.LogicOperator.OR);
            compositeFilter.addCondition(singleFilter);
            compositeFilter.addCondition(new RelationalCondition("Column1", RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromBoolean(false)));

            OTSException lengthExceedExp = new OTSException(
                    "The length of attribute column: 'Column' exceeded the MaxLength:65536 with CurrentLength:65537.",
                    null, "OTSParameterInvalid", "", 400);
            runAndExpectFail(singleFilter, lengthExceedExp);
            runAndExpectFail(compositeFilter, lengthExceedExp);
        }

        // BINARY
        {
            RowPrimaryKey primaryKey = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
		    criteria.setPrimaryKey(primaryKey);
             
            ColumnValue v = ColumnValue.fromBinary(new byte[64 * 1024 + 1]);
            RelationalCondition singleFilter = new RelationalCondition("Column", RelationalCondition.CompareOperator.EQUAL, v);
            CompositeCondition compositeFilter = new CompositeCondition(CompositeCondition.LogicOperator.OR);
            compositeFilter.addCondition(singleFilter);
            compositeFilter.addCondition(new RelationalCondition("Column1", RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromBoolean(false)));

            OTSException lengthExceedExp = new OTSException(
                    "The length of attribute column: 'Column' exceeded the MaxLength:65536 with CurrentLength:65537.",
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
        LOG.info("Start testCompareWithDifferentType");

        PutRowRequest pr = new PutRowRequest();

        RowPrimaryKey primaryKey =
                new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        RowPutChange rowChange = new RowPutChange(tableName);
        rowChange.setPrimaryKey(primaryKey);
        String[] names = new String[]{"col_int", "col_string", "col_double", "col_boolean", "col_byte"};
        ColumnValue[] values = new ColumnValue[] {ColumnValue.fromLong(0), ColumnValue.fromString("a"),
                ColumnValue.fromDouble(1.0), ColumnValue.fromBoolean(true), ColumnValue.fromBinary(new byte[]{0})};

        for (int i = 0; i < names.length; i++) {
            rowChange.addAttributeColumn(names[i], values[i]);
        }
        pr.setRowChange(rowChange);
        ots.putRow(pr);

        for (int i = 0; i < names.length; i++) {
            for (int j = 0; j < values.length; j++) {
                if (i == j) {
                    continue;
                }

                RelationalCondition singleFilter = new RelationalCondition(names[i], RelationalCondition.CompareOperator.EQUAL, values[j]);
                CompositeCondition compositeFilter = new CompositeCondition(CompositeCondition.LogicOperator.AND);
                compositeFilter.addCondition(singleFilter);
                compositeFilter.addCondition(new RelationalCondition("CC", RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromBoolean(false)));

                OTSException exp = new OTSException("The input parameter is invalid.", null, "OTSParameterInvalid", "", 400);

                runAndExpectFail(singleFilter, exp);
                runAndExpectFail(compositeFilter, exp);
            }
        }
    }

    /**
     * 构造 CompositeCondition 类型为 NOT，并且sub filter数量为2或9，期望出错。
     * 构造 CompositeCondition 类型为 AND/OR，并且sub filter数量为1，期望出错。
     */
    @Test
    public void testLogicOperatorRestriction() {
        LOG.info("Start testLogicOperatorRestriction");

        CompositeCondition orFilter = new CompositeCondition(CompositeCondition.LogicOperator.OR);
        CompositeCondition andFilter = new CompositeCondition(CompositeCondition.LogicOperator.AND);
        CompositeCondition notFilter = new CompositeCondition(CompositeCondition.LogicOperator.NOT);

        RelationalCondition singleFilter = new RelationalCondition("Column", RelationalCondition.CompareOperator.EQUAL, ColumnValue.fromBoolean(false));
        orFilter.addCondition(singleFilter);
        andFilter.addCondition(singleFilter);

        OTSException exp = new OTSException("The input parameter is invalid.", null, "OTSParameterInvalid", "", 400);
        runAndExpectFail(orFilter, exp);
        runAndExpectFail(andFilter, exp);

        for (int i = 0; i < 2; i++) {
            notFilter.addCondition(singleFilter);
        }
        runAndExpectFail(notFilter, exp);

        notFilter.clear();
        for (int i = 0; i < 9; i++) {
            notFilter.addCondition(singleFilter);
        }
        runAndExpectFail(notFilter, exp);
    }

    private void runAndExpectFail(ColumnCondition filter, OTSException exp) {
        // GetRow
        {
            RowPrimaryKey primaryKey = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
		    criteria.setPrimaryKey(primaryKey);
             
            criteria.setFilter(filter);

            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(criteria);

            try {
                ots.getRow(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(exp.getErrorCode(), exp.getMessage(), exp.getHttpStatus(), e);
            }
        }

        // GetRange
        {
            RowPrimaryKey begin = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
            RowPrimaryKey end = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10));

            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(begin);
            criteria.setExclusiveEndPrimaryKey(end);
            criteria.setFilter(filter);
             

            try {
                GetRangeRequest request = new GetRangeRequest();
                request.setRangeRowQueryCriteria(criteria);
                ots.getRange(request);
                fail();
            } catch (OTSException e) {
                assertOTSException(exp.getErrorCode(), exp.getMessage(), exp.getHttpStatus(), e);
            }
        }

        // BatchGetRow
        {
            RowPrimaryKey primaryKey = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
            MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
             
            criteria.addRow(primaryKey);
            criteria.setFilter(filter);

            try {
                BatchGetRowRequest request = new BatchGetRowRequest();
                request.addMultiRowQueryCriteria(criteria);
                BatchGetRowResult result = ots.batchGetRow(request);
                if (!result.isAllSucceed()) {
                    assertEquals(result.getFailedRows().size(), 1);
                    assertEquals(result.getFailedRows().get(0).getError().getCode(), exp.getErrorCode());
                    assertEquals(result.getFailedRows().get(0).getError().getMessage(), exp.getMessage());
                } else {
                    fail();
                }
            } catch (OTSException e) {
                assertOTSException(exp.getErrorCode(), exp.getMessage(), exp.getHttpStatus(), e);
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
        LOG.info("Start testColumnNameMaxLength");

        RowPrimaryKey primaryKey =
                new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        ColumnValue columnValue = ColumnValue.fromString(NewString(2 * 896, 'c'));

        for (int i = 0; i < 16; i++) {
            UpdateRowRequest pr = new UpdateRowRequest();
            RowUpdateChange rowChange = new RowUpdateChange(tableName);
            rowChange.setPrimaryKey(primaryKey);
            for (int j = 0; j < 8; j++) {
                rowChange.addAttributeColumn(String.format("C%0254d", i * 8 + j), columnValue);
            }

            pr.setRowChange(rowChange);
            ots.updateRow(pr);
        }

        {
            CompositeCondition compositeFilter = new CompositeCondition(CompositeCondition.LogicOperator.OR);
            for (int i = 0; i < 9; i++) {
                compositeFilter.addCondition(new RelationalCondition(String.format("C%0254d", i), RelationalCondition.CompareOperator.EQUAL, columnValue));
            }
            testFilterWithMaxNameLength(compositeFilter);
        }
        {
            CompositeCondition compositeFilter = new CompositeCondition(CompositeCondition.LogicOperator.AND);
            for (int i = 0; i < 9; i++) {
                compositeFilter.addCondition(new RelationalCondition(String.format("C%0254d", i), RelationalCondition.CompareOperator.EQUAL, columnValue));
            }
            testFilterWithMaxNameLength(compositeFilter);
        }
    }

    private void testFilterWithMaxNameLength(CompositeCondition compositeFilter) {
        LOG.info("Start testFilterWithMaxNameLength");

        RowPrimaryKey primaryKey =
                new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        ColumnValue columnValue = ColumnValue.fromString(NewString(2 * 896, 'c'));

        List<Row> rows = new ArrayList<Row>();

        LOG.info("get row");
        {

            GetRowRequest request = new GetRowRequest();
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
		    criteria.setPrimaryKey(primaryKey);
             
            criteria.setFilter(compositeFilter);
            request.setRowQueryCriteria(criteria);

            GetRowResult result = ots.getRow(request);
            rows.add(result.getRow());
        }

        LOG.info("get range");
        {
            RowPrimaryKey begin = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
            RowPrimaryKey end = new RowPrimaryKey().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10));

            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(begin);
            criteria.setExclusiveEndPrimaryKey(end);
             
            criteria.setFilter(compositeFilter);

            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);
            GetRangeResult result = ots.getRange(request);
            assertEquals(result.getRows().size(), 1);
            rows.add(result.getRows().get(0));
        }

        LOG.info("batch get row");
        {
            MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
             
            criteria.addRow(primaryKey);
            criteria.setFilter(compositeFilter);

            BatchGetRowRequest request = new BatchGetRowRequest();
            request.addMultiRowQueryCriteria(criteria);

            BatchGetRowResult result = ots.batchGetRow(request);
            assertTrue(result.isAllSucceed());
            assertEquals(result.getSucceedRows().size(), 1);
            rows.add(result.getSucceedRows().get(0).getRow());
        }

        LOG.info("check result");
        for (Row row : rows) {
            Row expect = new Row();
            expect.addColumn("PK0", ColumnValue.fromLong(0));
            for (int i = 0; i < 128; i++) {
                expect.addColumn(String.format("C%0254d", i), ColumnValue.fromString(NewString(2 * 896, 'c')));
            }
            checkRow(expect, row);
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
