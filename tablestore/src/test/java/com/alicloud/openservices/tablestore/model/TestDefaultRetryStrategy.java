package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.PartialResultFailedException;
import com.alicloud.openservices.tablestore.TableStoreException;
import static com.alicloud.openservices.tablestore.core.ErrorCode.*;
import static com.alicloud.openservices.tablestore.model.OperationNames.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDefaultRetryStrategy {
	
	private static List<String> allOperations;
	private static List<String> idempotentOperations;
	private static List<String> unidempotentOperations;
	private static Set<String> test;
	private Random rnd = new Random();
    private static Logger LOG = Logger.getLogger(TestDefaultRetryStrategy.class.getName());
	
	@BeforeClass
	public static void classBefore() {
		allOperations = new ArrayList<String>();
		allOperations.add(OP_CREATE_TABLE);
		allOperations.add(OP_LIST_TABLE);
		allOperations.add(OP_DELETE_TABLE);
		allOperations.add(OP_DESCRIBE_TABLE);
		allOperations.add(OP_UPDATE_TABLE);
		allOperations.add(OP_GET_ROW);
		allOperations.add(OP_PUT_ROW);
		allOperations.add(OP_UPDATE_ROW);
		allOperations.add(OP_DELETE_ROW);
		allOperations.add(OP_BATCH_GET_ROW);
		allOperations.add(OP_BATCH_WRITE_ROW);
		allOperations.add(OP_GET_RANGE);

		idempotentOperations = new ArrayList<String>();
		idempotentOperations.add(OP_LIST_TABLE);
		idempotentOperations.add(OP_DESCRIBE_TABLE);
		idempotentOperations.add(OP_GET_ROW);
		idempotentOperations.add(OP_BATCH_GET_ROW);
		idempotentOperations.add(OP_GET_RANGE);
		
		unidempotentOperations = new ArrayList<String>();
		unidempotentOperations.add(OP_CREATE_TABLE);
		unidempotentOperations.add(OP_DELETE_TABLE);
		unidempotentOperations.add(OP_UPDATE_TABLE);
		unidempotentOperations.add(OP_PUT_ROW);
		unidempotentOperations.add(OP_UPDATE_ROW);
		unidempotentOperations.add(OP_DELETE_ROW);
		unidempotentOperations.add(OP_BATCH_WRITE_ROW);
	}

	public void testTableStoreCommon(String action, String msg, String errorCode, int httpStatus, boolean expectRetry) {
		LOG.info("Action:" + action + ", msg:" + msg + ", errorCode:" + errorCode + ", httpStatus:" + String.valueOf(httpStatus));
		TableStoreException e = new TableStoreException(msg, null, errorCode, "requestId", httpStatus);
		DefaultRetryStrategy retry = new DefaultRetryStrategy();
		assertEquals(retry.shouldRetry(action, e), expectRetry);
	}
	
	public void testClientCommon(String action, boolean expectRetry) {
		LOG.info("Action:" + action);
		ClientException e = new ClientException();
		DefaultRetryStrategy retry = new DefaultRetryStrategy();
		assertEquals(retry.shouldRetry(action, e), expectRetry);
	}
	
	/**
	 * If the exception is TableStoreException and the error code is OTSRowOperationConflict, OTSNotEnoughCapacityUnit, OTSTableNotReady, 
	 * OTSPartitionUnavailable, or OTSServerBusy, then you can retry.
	 * @throws Exception
	 */
	@Test
	public void testCase1() throws Exception {
		List<String> acceptErrors = new ArrayList<String>();
		acceptErrors.add(ROW_OPERATION_CONFLICT);
		acceptErrors.add(NOT_ENOUGH_CAPACITY_UNIT);
		acceptErrors.add(TABLE_NOT_READY);
		acceptErrors.add(PARTITION_UNAVAILABLE);
		acceptErrors.add(SERVER_BUSY);
		
		testTableStoreCommon(allOperations.get(rnd.nextInt(allOperations.size())), "test", 
				acceptErrors.get(rnd.nextInt(acceptErrors.size())), 400, true);
	}
	
	/**
	 * If the exception is OTSQuotaExhausted and the exception message is "Too frequent table operations.", then you can retry.
	 * @throws Exception
	 */
	@Test
	public void testCase2() throws Exception {
		testTableStoreCommon(allOperations.get(rnd.nextInt(allOperations.size())), 
				"Too frequent table operations.", 
				QUOTA_EXHAUSTED, 400, true);
	}
	
	/**
	 * If the exception is OTSQuotaExhausted and the exception message is not "Too frequent table operations.", retry is not allowed.
	 * @throws Exception
	 */
	@Test
	public void testCase3() throws Exception {
		testTableStoreCommon(allOperations.get(rnd.nextInt(allOperations.size())), 
				"error message", 
				QUOTA_EXHAUSTED, 400, false);
	}
	
	/**
	 * If the exception is a ClientException (network-related exception) and the operation is idempotent, you can retry.
	 * @throws Exception
	 */
	@Test
	public void testCase4() throws Exception {
		testClientCommon(idempotentOperations.get(rnd.nextInt(idempotentOperations.size())), true);
	}
	
	/**
	 * If the exception is a ClientException (network-related exception) and the operation is not idempotent, retry is not allowed.
	 * @throws Exception
	 */
	@Test
	public void testCase5() throws Exception {
		testClientCommon(unidempotentOperations.get(rnd.nextInt(unidempotentOperations.size())), false);
	}
	
	/**
	 * If the exception is OTSTimeout, OTSInternalServerError, OTSServerUnavaillable, or the HTTP status code is 500, 502, or 503, and the operation is idempotent, then it can be retried.
	 * @throws Exception
	 */
	@Test
	public void testCase6() throws Exception {
		List<String> acceptErrors = new ArrayList<String>();
		acceptErrors.add(STORAGE_TIMEOUT);
		acceptErrors.add(INTERNAL_SERVER_ERROR);
		acceptErrors.add(SERVER_UNAVAILABLE);
		
		testTableStoreCommon(idempotentOperations.get(rnd.nextInt(idempotentOperations.size())), "test", 
				acceptErrors.get(rnd.nextInt(acceptErrors.size())), 400, true);
		
		int errorCode = 500 + rnd.nextInt(100);
		testTableStoreCommon(idempotentOperations.get(rnd.nextInt(idempotentOperations.size())), "test", 
				CONDITION_CHECK_FAIL, errorCode, true);
	}
	
	/**
	 * If the exception is OTSTimeout, OTSInternalServerError, OTSServerUnavaillable, or the HTTP status code is 500, 502, or 503, and the operation is not idempotent, then it should not be retried.
	 * @throws Exception
	 */
	@Test
	public void testCase7() throws Exception {
		List<String> acceptErrors = new ArrayList<String>();
		acceptErrors.add(STORAGE_TIMEOUT);
		acceptErrors.add(INTERNAL_SERVER_ERROR);
		acceptErrors.add(SERVER_UNAVAILABLE);
		
		testTableStoreCommon(unidempotentOperations.get(rnd.nextInt(unidempotentOperations.size())), "test", 
				acceptErrors.get(rnd.nextInt(acceptErrors.size())), 400, false);
		
		int errorCode = 500 + rnd.nextInt(100);
		testTableStoreCommon(unidempotentOperations.get(rnd.nextInt(unidempotentOperations.size())), "test", 
				CONDITION_CHECK_FAIL, errorCode, false);
	}
	
	/**
	 * For Batch operations, the entire batch operation can only be retried if all failed rows are retryable.
	 * @throws Exception
	 */
	@Test
	public void testCase8() throws Exception {
		PartialResultFailedException partialEx = new PartialResultFailedException(null, "test", null);
		String action = OP_BATCH_WRITE_ROW;
		TableStoreException exCanRetry = new TableStoreException("test", null, ROW_OPERATION_CONFLICT, "requestId", 500);
		TableStoreException exCannotRetry = new TableStoreException("test", null, CONDITION_CHECK_FAIL, "requestId", 400);
		
		DefaultRetryStrategy retry = new DefaultRetryStrategy();
		
		// All failed rows are retryable.
		partialEx.addError(exCanRetry);
		partialEx.addError(exCanRetry);
		assertEquals(retry.shouldRetry(action, partialEx), true);
		
		// Rows that partially fail can be retried.
		partialEx.addError(exCannotRetry);
		assertEquals(retry.shouldRetry(action, partialEx), false);
		
	}
}
