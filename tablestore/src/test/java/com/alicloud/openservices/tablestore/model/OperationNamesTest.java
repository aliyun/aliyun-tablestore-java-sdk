package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.*;

public class OperationNamesTest {

    @Test
    public void testComputeSplits(){
        assertEquals("ComputeSplits", OperationNames.OP_COMPUTE_SPLITS);
    }

}