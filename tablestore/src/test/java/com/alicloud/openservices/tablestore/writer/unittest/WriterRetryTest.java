package com.alicloud.openservices.tablestore.writer.unittest;


import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.PartialResultFailedException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.writer.retry.CertainCodeNotRetryStrategy;
import com.alicloud.openservices.tablestore.writer.retry.CertainCodeRetryStrategy;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.alicloud.openservices.tablestore.model.OperationNames.OP_BULK_IMPORT;

public class WriterRetryTest {

    private static String UN_EXPECTED_ERROR_CODE = "UnExpectedErrorCode";
    private List<String> certainRetryCode = Arrays.asList(
            "OTSInternalServerError", "OTSRequestTimeout", "OTSPartitionUnavailable", "OTSTableNotReady",
            "OTSRowOperationConflict", "OTSTimeout", "OTSServerUnavailable", "OTSServerBusy");
    private List<String> certainNotRetryCode = Arrays.asList(
            "OTSParameterInvalid", "OTSConditionCheckFail", "OTSRequestBodyTooLarge",
            "OTSInvalidPK", "OTSOutOfColumnCountLimit", "OTSOutOfRowSizeLimit");

    @Test
    public void testCertainCodeRetry() {
        RetryStrategy retryStrategy = new CertainCodeRetryStrategy();

        /**
         * Not TableStoreException                                Not retriable
         * */
        long nextPause = retryStrategy.clone().nextPause(OP_BULK_IMPORT, new Exception());
        Assert.assertEquals(nextPause, 0);

        /**
         * ClientException                                      Retriable
         * */
        nextPause = retryStrategy.clone().nextPause(OP_BULK_IMPORT, new ClientException());
        Assert.assertTrue(nextPause > 0);

        /**
         * 5XX errors, not PartitionFailed                            Retriable
         * */
        nextPause = retryStrategy.clone().nextPause(OP_BULK_IMPORT, new TableStoreException("message", null, UN_EXPECTED_ERROR_CODE, null,  501));
        Assert.assertTrue(nextPause > 0);

        /**
         * PartialResultFailedException, and each row is retryable.     Overall, it is retryable.
         *
         */
        PartialResultFailedException exception = new PartialResultFailedException(null, "requestId", null);
        for (String errorCode : certainRetryCode) {
            exception.addError(new TableStoreException("", errorCode));
        }
        nextPause = retryStrategy.clone().nextPause(OP_BULK_IMPORT, exception);
        Assert.assertTrue(nextPause > 0);

        /**
         * PartialResultFailedException, some rows are not retryable,            overall not retryable
         *
         * */
        exception.addError(new TableStoreException("", UN_EXPECTED_ERROR_CODE));
        nextPause = retryStrategy.clone().nextPause(OP_BULK_IMPORT, exception);
        Assert.assertTrue(nextPause == 0);

    }

    @Test
    public void testCertainCodeNotRetry() {
        RetryStrategy retryStrategy = new CertainCodeNotRetryStrategy();

        /**
         * Not TableStoreException                                Retriable
         * */
        long nextPause = retryStrategy.clone().nextPause(OP_BULK_IMPORT, new Exception());
        Assert.assertEquals(nextPause, 0);

        /**
         * ClientException                                       Retriable
         * */
        nextPause = retryStrategy.clone().nextPause(OP_BULK_IMPORT, new ClientException());
        Assert.assertTrue(nextPause > 0);

        /**
         * 5XX errors                                                Retriable
         * */
        nextPause = retryStrategy.clone().nextPause(OP_BULK_IMPORT, new TableStoreException("message", null, UN_EXPECTED_ERROR_CODE, null,  501));
        Assert.assertTrue(nextPause > 0);

        /**
         * Not 5XX, not non-retry error codes                              Retriable
         * */
        nextPause = retryStrategy.clone().nextPause(OP_BULK_IMPORT, new TableStoreException("message", null, UN_EXPECTED_ERROR_CODE, null,  400));
        Assert.assertTrue(nextPause > 0);

        /**
         * PartialResultFailedException, each row is retryable       Overall is retryable
         *
         * */
        PartialResultFailedException exception = new PartialResultFailedException(null, "requestId", null);
        for (String errorCode : certainRetryCode) {
            exception.addError(new TableStoreException("", errorCode));
        }
        exception.addError(new TableStoreException("", UN_EXPECTED_ERROR_CODE));
        nextPause = retryStrategy.clone().nextPause(OP_BULK_IMPORT, exception);
        Assert.assertTrue(nextPause > 0);

        /**
         * PartialResultFailedException, some rows are not retryable,             overall not retryable
         *
         * */
        for (String errorCode : certainNotRetryCode) {
            exception.addError(new TableStoreException("", errorCode));
        }
        nextPause = retryStrategy.clone().nextPause(OP_BULK_IMPORT, exception);
        Assert.assertTrue(nextPause == 0);

    }
}
