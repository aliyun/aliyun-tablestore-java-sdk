package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.OperationNames;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CreateTimeseriesLastpointIndexRequestTest {

    @Test
    public void testToString() {
        // Test case design: Test the toString method to ensure that it returns a string in the expected format.
        CreateTimeseriesLastpointIndexRequest request = new CreateTimeseriesLastpointIndexRequest("test_table", "lastpoint_index", true);
        assertEquals(
                "CreateTimeseriesLastpointIndexRequest{timeseriesTableName='test_table', lastpointIndexName='lastpoint_index', includeBaseData=true, createOnWideColumnTable=null, lastpointIndexPrimaryKeyNames=null}",
                request.toString());
    }

    @Test
    public void testGetOperationName() {
        // Test case design: Test the getOperationName method to ensure that it returns the correct operation name.
        CreateTimeseriesLastpointIndexRequest request = new CreateTimeseriesLastpointIndexRequest("test_table", "lastpoint_index", true);
        assertEquals(OperationNames.OP_CREATE_TIMESERIES_LASTPOINT_INDEX, request.getOperationName());
    }

    @Test
    public void testGetTimeseriesTableName() {
        // Test case design: Test the getTimeseriesTableName method to ensure that it returns the correct table name.
        CreateTimeseriesLastpointIndexRequest request = new CreateTimeseriesLastpointIndexRequest("test_table", "lastpoint_index", true);
        assertEquals("test_table", request.getTimeseriesTableName());
    }

    @Test
    public void testGetLastpointIndexName() {
        // Test case design: Test the getLastpointIndexName method to ensure that it returns the correct index name.
        CreateTimeseriesLastpointIndexRequest request = new CreateTimeseriesLastpointIndexRequest("test_table", "lastpoint_index", true);
        assertEquals("lastpoint_index", request.getLastpointIndexName());
    }

    @Test
    public void testIsIncludeBaseData() {
        // Test case design: Test the isIncludeBaseData method to ensure that it returns the correct value for the include base data flag.
        CreateTimeseriesLastpointIndexRequest request1 = new CreateTimeseriesLastpointIndexRequest("test_table", "lastpoint_index", true);
        CreateTimeseriesLastpointIndexRequest request2 = new CreateTimeseriesLastpointIndexRequest("test_table", "lastpoint_index", false);

        assertEquals(true, request1.isIncludeBaseData());
        assertEquals(false, request2.isIncludeBaseData());
    }
}
